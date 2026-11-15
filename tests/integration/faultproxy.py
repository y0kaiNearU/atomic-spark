"""HTTP proxy in front of Polaris that can break atomic-spark's publish commit in controlled ways.

Every request is forwarded unchanged, except publish commits (POST .../transactions/commit that move a
table's main) while a fault is armed:

    drop             close the connection without forwarding: the commit never reaches Polaris
    apply_then_drop  forward, wait for Polaris to apply it, then close the connection without a response
    apply_then_504   forward, then answer 504 Gateway Timeout instead of Polaris's response
    hold             keep the request until release() is called, then forward it: models a request
                     stuck in the network or server queue while the client has already timed out

Spark's own writes (per-table commits) and atomic-spark's branch create/drop commits pass through.
"""

from __future__ import annotations

import http.client
import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

MODES = ("drop", "apply_then_drop", "apply_then_504", "hold")
_HOP_BY_HOP = {"connection", "keep-alive", "proxy-connection", "transfer-encoding", "te", "trailer", "upgrade",
               "host", "content-length"}


@dataclass
class Late:
    """What happened to a held publish commit once it was finally forwarded."""
    released: threading.Event = field(default_factory=threading.Event)
    done: threading.Event = field(default_factory=threading.Event)
    status: int | None = None
    body: str = ""


class FaultProxy:
    def __init__(self, upstream: str):
        u = urlsplit(upstream)
        self._upstream = (u.hostname, u.port or 80)
        self._lock = threading.Lock()
        self._mode: str | None = None
        self._times = 0
        self.publish_requests = 0  # publish commits seen, faulty or not
        self.late = Late()
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # keep pytest output clean
                pass

            def _handle(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                fault = proxy._take_fault(self.command, self.path, body)
                if fault == "drop":
                    self.close_connection = True
                    return
                if fault == "hold":
                    late = proxy.late
                    threading.Thread(target=proxy._forward_late,
                                     args=(late, self.command, self.path, dict(self.headers), body), daemon=True).start()
                    late.done.wait(600)  # the client gives up first; then just close
                    self.close_connection = True
                    return
                status, reason, headers, payload = proxy._forward(self.command, self.path, dict(self.headers), body)
                if fault == "apply_then_drop":
                    self.close_connection = True
                    return
                if fault == "apply_then_504":
                    status, reason, headers = 504, "Gateway Timeout", [("Content-Type", "application/json")]
                    payload = json.dumps({"error": {"message": "injected 504", "type": "GatewayTimeout", "code": 504}}).encode()
                self.send_response(status, reason)
                for k, v in headers:
                    if k.lower() not in _HOP_BY_HOP:
                        self.send_header(k, v)
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(payload)
                self.close_connection = True

            do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = _handle

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def start(self) -> "FaultProxy":
        self._thread.start()
        return self

    def stop(self) -> None:
        self.late.released.set()
        self._server.shutdown()
        self._server.server_close()

    # -- control ---------------------------------------------------------------
    def arm(self, mode: str, times: int = 1) -> None:
        """Break the next `times` publish commits."""
        assert mode in MODES, mode
        with self._lock:
            self._mode, self._times, self.publish_requests, self.late = mode, times, 0, Late()

    def disarm(self) -> None:
        with self._lock:
            self._mode, self._times = None, 0
        self.late.released.set()

    def release(self, timeout: float = 60) -> Late:
        """Let a held publish commit through and wait for Polaris's answer."""
        self.late.released.set()
        assert self.late.done.wait(timeout), "held commit was never forwarded"
        return self.late

    # -- internals -------------------------------------------------------------
    def _take_fault(self, method: str, path: str, body: bytes) -> str | None:
        if method != "POST" or not path.split("?")[0].endswith("/transactions/commit") or not _is_publish(body):
            return None
        with self._lock:
            self.publish_requests += 1
            if self._mode is None or self._times <= 0:
                return None
            self._times -= 1
            return self._mode

    def _forward(self, method, path, headers, body):
        conn = http.client.HTTPConnection(*self._upstream, timeout=120)
        try:
            fwd = {k: v for k, v in headers.items() if k.lower() not in _HOP_BY_HOP}
            fwd["Host"] = f"{self._upstream[0]}:{self._upstream[1]}"
            conn.request(method, path, body=body or None, headers=fwd)
            r = conn.getresponse()
            return r.status, r.reason, r.getheaders(), r.read()
        finally:
            conn.close()

    def _forward_late(self, late: Late, method, path, headers, body):
        late.released.wait(600)
        try:
            late.status, _, _, payload = self._forward(method, path, headers, body)
            late.body = payload.decode(errors="replace")
        finally:
            late.done.set()


def _is_publish(body: bytes) -> bool:
    try:
        changes = json.loads(body).get("table-changes", [])
    except ValueError:
        return False
    return any(u.get("action") == "set-snapshot-ref" and u.get("ref-name") == "main"
               for ch in changes for u in ch.get("updates", []))
