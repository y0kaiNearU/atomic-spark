"""Minimal Iceberg REST catalog client: table loads and multi-table commits."""

from __future__ import annotations

import threading
import time
from urllib.parse import quote

import requests

from .config import CatalogConfig
from .errors import CatalogError
from .model import TableRef

_TOKEN_REFRESH_MARGIN_S = 60


class RestCatalogClient:
    def __init__(
        self,
        uri: str,
        *,
        warehouse: str | None = None,
        credential: str | None = None,
        token: str | None = None,
        scope: str = "catalog",
        oauth2_server_uri: str | None = None,
        headers: dict[str, str] | None = None,
        session: requests.Session | None = None,
        timeout: float = 60.0,
    ):
        self.uri = uri.rstrip("/")
        self.warehouse = warehouse
        self.credential = credential
        self.scope = scope
        self.oauth2_server_uri = oauth2_server_uri or f"{self.uri}/v1/oauth/tokens"
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update(headers or {})
        self._static_token = token
        self._token: str | None = None
        self._token_expires_at = 0.0
        self._base: str | None = None
        self._lock = threading.Lock()

    @classmethod
    def from_config(cls, cfg: CatalogConfig, **kwargs) -> "RestCatalogClient":
        return cls(
            cfg.uri,
            warehouse=cfg.warehouse,
            credential=cfg.credential,
            token=cfg.token,
            scope=cfg.scope,
            oauth2_server_uri=cfg.oauth2_server_uri,
            headers=cfg.headers,
            **kwargs,
        )

    # -- auth / routing -------------------------------------------------------
    def _bearer(self) -> str:
        if self._static_token:
            return self._static_token
        with self._lock:
            if self._token is None or time.monotonic() >= self._token_expires_at:
                self._fetch_token()
            return self._token  # type: ignore[return-value]

    def _fetch_token(self) -> None:
        # Iceberg credential format: "client_id:client_secret" or just "client_secret".
        cred = self.credential or ""
        client_id, sep, secret = cred.partition(":")
        if not sep:
            client_id, secret = "", cred
        data = {"grant_type": "client_credentials", "client_secret": secret, "scope": self.scope}
        if client_id:
            data["client_id"] = client_id
        r = self.session.post(self.oauth2_server_uri, data=data, timeout=self.timeout)
        if r.status_code >= 400:
            raise _error(r)
        body = r.json()
        self._token = body["access_token"]
        expires_in = float(body.get("expires_in", 3600))
        self._token_expires_at = time.monotonic() + max(expires_in - _TOKEN_REFRESH_MARGIN_S, expires_in / 2)

    def _base_url(self) -> str:
        if self._base is None:
            params = {"warehouse": self.warehouse} if self.warehouse else None
            cfg = self._request("GET", f"{self.uri}/v1/config", params=params).json()
            prefix = (cfg.get("overrides") or {}).get("prefix") or (cfg.get("defaults") or {}).get("prefix")
            self._base = f"{self.uri}/v1/{quote(prefix, safe='')}" if prefix else f"{self.uri}/v1"
        return self._base

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        headers = {"Authorization": f"Bearer {self._bearer()}"}
        r = self.session.request(method, url, headers=headers, timeout=self.timeout, **kwargs)
        if r.status_code >= 400:
            raise _error(r)
        return r

    def _table_url(self, ref: TableRef) -> str:
        ns = quote("\x1f".join(ref.namespace), safe="")
        return f"{self._base_url()}/namespaces/{ns}/tables/{quote(ref.name, safe='')}"

    # -- API ------------------------------------------------------------------
    def load_table(self, ref: TableRef) -> dict:
        """Return the table's metadata (the "metadata" field of LoadTableResult)."""
        return self._request("GET", self._table_url(ref)).json()["metadata"]

    def commit_transaction(self, table_changes: list[dict]) -> None:
        """POST /transactions/commit: apply every change atomically, or none."""
        self._request("POST", f"{self._base_url()}/transactions/commit", json={"table-changes": table_changes})

    def commit_table(self, ref: TableRef, requirements: list[dict], updates: list[dict]) -> None:
        body = {"identifier": ref.rest_identifier(), "requirements": requirements, "updates": updates}
        self._request("POST", self._table_url(ref), json=body)


def _error(r: requests.Response) -> CatalogError:
    error_type, message = None, r.text[:1000]
    try:
        err = r.json().get("error", {})
        error_type, message = err.get("type"), err.get("message", message)
    except ValueError:
        pass
    return CatalogError(r.status_code, error_type, message, r.text[:2000])


# -- payload helpers ------------------------------------------------------------


def table_change(ref: TableRef, requirements: list[dict], updates: list[dict]) -> dict:
    return {"identifier": ref.rest_identifier(), "requirements": requirements, "updates": updates}


def assert_uuid(uuid: str) -> dict:
    return {"type": "assert-table-uuid", "uuid": uuid}


def assert_ref(ref: str, snapshot_id: int | None) -> dict:
    """snapshot_id=None asserts that the ref does not exist."""
    return {"type": "assert-ref-snapshot-id", "ref": ref, "snapshot-id": snapshot_id}


def assert_schema_id(schema_id: int) -> dict:
    return {"type": "assert-current-schema-id", "current-schema-id": schema_id}


def set_branch(name: str, snapshot_id: int, max_ref_age_ms: int | None = None) -> dict:
    upd = {"action": "set-snapshot-ref", "ref-name": name, "type": "branch", "snapshot-id": snapshot_id}
    if max_ref_age_ms is not None:
        upd["max-ref-age-ms"] = max_ref_age_ms
    return upd


def remove_ref(name: str) -> dict:
    return {"action": "remove-snapshot-ref", "ref-name": name}
