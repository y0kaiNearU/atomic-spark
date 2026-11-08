"""Minimal Iceberg REST client for the M0 spike (Polaris)."""

from __future__ import annotations

import requests


class RestError(Exception):
    def __init__(self, status: int, body: str):
        super().__init__(f"HTTP {status}: {body[:500]}")
        self.status = status
        self.body = body


class IcebergRest:
    def __init__(self, uri: str, credential: str, warehouse: str, scope: str = "PRINCIPAL_ROLE:ALL"):
        self.uri = uri.rstrip("/")
        client_id, client_secret = credential.split(":", 1)
        tok = requests.post(
            f"{self.uri}/v1/oauth/tokens",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": scope,
            },
            timeout=30,
        )
        tok.raise_for_status()
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {tok.json()['access_token']}"
        cfg = self._req("GET", f"{self.uri}/v1/config", params={"warehouse": warehouse}).json()
        self.prefix = cfg.get("overrides", {}).get("prefix") or cfg.get("defaults", {}).get("prefix")
        self.base = f"{self.uri}/v1/{self.prefix}" if self.prefix else f"{self.uri}/v1"

    def _req(self, method: str, url: str, **kw) -> requests.Response:
        r = self.session.request(method, url, timeout=60, **kw)
        if r.status_code >= 400:
            raise RestError(r.status_code, r.text)
        return r

    # -- tables ---------------------------------------------------------------
    def load_table(self, ns: str, name: str) -> dict:
        return self._req("GET", f"{self.base}/namespaces/{ns}/tables/{name}").json()["metadata"]

    def table_exists(self, ns: str, name: str) -> bool:
        r = self.session.head(f"{self.base}/namespaces/{ns}/tables/{name}", timeout=30)
        return r.status_code in (200, 204)

    def refs(self, ns: str, name: str) -> dict[str, int]:
        md = self.load_table(ns, name)
        return {k: v["snapshot-id"] for k, v in (md.get("refs") or {}).items()}

    def stage_create(self, ns: str, name: str, schema: dict, properties: dict | None = None) -> dict:
        body = {"name": name, "schema": schema, "stage-create": True, "properties": properties or {}}
        return self._req("POST", f"{self.base}/namespaces/{ns}/tables", json=body).json()

    def commit_table(self, ns: str, name: str, requirements: list[dict], updates: list[dict]) -> requests.Response:
        body = {"identifier": ident(ns, name), "requirements": requirements, "updates": updates}
        return self._req("POST", f"{self.base}/namespaces/{ns}/tables/{name}", json=body)

    # -- multi-table commit ---------------------------------------------------
    def commit_transaction(self, table_changes: list[dict]) -> requests.Response:
        return self._req("POST", f"{self.base}/transactions/commit", json={"table-changes": table_changes})


def ident(ns: str, name: str) -> dict:
    return {"namespace": [ns], "name": name}


def assert_ref(ref: str, snapshot_id: int | None) -> dict:
    return {"type": "assert-ref-snapshot-id", "ref": ref, "snapshot-id": snapshot_id}


def set_ref(ref: str, snapshot_id: int) -> dict:
    return {"action": "set-snapshot-ref", "ref-name": ref, "type": "branch", "snapshot-id": snapshot_id}


def change(ns: str, name: str, requirements: list[dict], updates: list[dict]) -> dict:
    return {"identifier": ident(ns, name), "requirements": requirements, "updates": updates}
