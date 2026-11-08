"""In-memory stand-ins for the Iceberg REST catalog and the Spark session operations.

FakeCatalog applies multi-table commits all-or-nothing and checks requirements the way Polaris does
(verified in spike/FINDINGS.md). Failures can be injected before or after a commit is applied.
"""

from __future__ import annotations

import copy
import itertools
import uuid

from atomic_spark.errors import CatalogError
from atomic_spark.model import TableRef, main_snapshot

CATALOG = "cat"

BASE_SCHEMA = {
    "type": "struct",
    "schema-id": 0,
    "fields": [
        {"id": 1, "name": "id", "required": False, "type": "long"},
        {"id": 2, "name": "v", "required": False, "type": "string"},
    ],
}


def ref(name: str) -> TableRef:
    return TableRef.parse(name, CATALOG)


class FakeCatalog:
    def __init__(self):
        self.tables: dict[TableRef, dict] = {}
        self.commits: list[list[dict]] = []
        self.failures: list[tuple[str, Exception]] = []  # ("before" | "after", exception) for upcoming commits
        self._ids = itertools.count(1000)

    # -- setup / simulation ---------------------------------------------------
    def create(self, name: str, *, wap: bool = True, rows: bool = True) -> TableRef:
        r = ref(name)
        self.tables[r] = {
            "table-uuid": str(uuid.uuid4()),
            "properties": {"write.wap.enabled": "true"} if wap else {},
            "refs": {},
            "current-snapshot-id": -1,
            "snapshots": [],
            "current-schema-id": 0,
            "schemas": [copy.deepcopy(BASE_SCHEMA)],
        }
        if rows:
            self.write(r, "main")
        return r

    def write(self, r: TableRef, branch: str) -> int:
        md = self.tables[r]
        parent = md["refs"][branch]["snapshot-id"] if branch in md["refs"] else main_snapshot(md)
        sid = next(self._ids)
        md["snapshots"].append({"snapshot-id": sid, "parent-snapshot-id": parent})
        md["refs"][branch] = {"snapshot-id": sid, "type": "branch"}
        if branch == "main":
            md["current-snapshot-id"] = sid
        return sid

    def evolve_schema(self, r: TableRef, fields: list[dict]) -> None:
        md = self.tables[r]
        sid = max(s["schema-id"] for s in md["schemas"]) + 1
        md["schemas"].append({"type": "struct", "schema-id": sid, "fields": fields})
        md["current-schema-id"] = sid

    def main(self, name: str) -> int | None:
        return main_snapshot(self.tables[ref(name)])

    def refs(self, name: str) -> dict[str, int]:
        return {k: v["snapshot-id"] for k, v in self.tables[ref(name)]["refs"].items()}

    # -- client interface -----------------------------------------------------
    def load_table(self, r: TableRef) -> dict:
        if r not in self.tables:
            raise CatalogError(404, "NoSuchTableException", f"Table does not exist: {r}")
        return copy.deepcopy(self.tables[r])

    def commit_table(self, r: TableRef, requirements: list[dict], updates: list[dict]) -> None:
        self.commit_transaction([{"identifier": r.rest_identifier(), "requirements": requirements, "updates": updates}])

    def commit_transaction(self, table_changes: list[dict]) -> None:
        self.commits.append(copy.deepcopy(table_changes))
        when, exc = self.failures.pop(0) if self.failures else (None, None)
        if when == "before":
            raise exc
        staged = copy.deepcopy(self.tables)
        for ch in table_changes:
            r = TableRef(tuple(ch["identifier"]["namespace"]), ch["identifier"]["name"])
            if r not in staged:
                raise CatalogError(404, "NoSuchTableException", f"Table does not exist: {r}")
            md = staged[r]
            for req in ch["requirements"]:
                _check(md, req)
            for upd in ch["updates"]:
                _apply(md, upd)
        self.tables = staged
        if when == "after":
            raise exc


def _conflict(msg: str) -> CatalogError:
    return CatalogError(409, "CommitFailedException", f"Requirement failed: {msg}")


def _check(md: dict, req: dict) -> None:
    t = req["type"]
    if t == "assert-table-uuid" and md["table-uuid"] != req["uuid"]:
        raise _conflict("table uuid changed")
    if t == "assert-ref-snapshot-id":
        cur = md["refs"].get(req["ref"])
        cur_id = None if cur is None else cur["snapshot-id"]
        if cur_id != req["snapshot-id"]:
            raise _conflict(f"branch {req['ref']} has changed: expected id {req['snapshot-id']} != {cur_id}")
    if t == "assert-current-schema-id" and md["current-schema-id"] != req["current-schema-id"]:
        raise _conflict("current schema changed")


def _apply(md: dict, upd: dict) -> None:
    a = upd["action"]
    if a == "set-snapshot-ref":
        md["refs"][upd["ref-name"]] = {k: v for k, v in upd.items() if k not in ("action", "ref-name")}
        if upd["ref-name"] == "main":
            md["current-snapshot-id"] = upd["snapshot-id"]
    elif a == "remove-snapshot-ref":
        md["refs"].pop(upd["ref-name"], None)
    else:
        raise AssertionError(f"FakeCatalog does not support {a}")


class FakeOps:
    """Spark session stand-in. write() mimics Iceberg: with WAP set on a WAP-enabled table, write the branch."""

    def __init__(self, catalog: FakeCatalog):
        self.catalog = catalog
        self.wap: str | None = None
        self.refreshed: list[TableRef] = []
        self.enabled: list[TableRef] = []

    def current_wap(self):
        return self.wap

    def set_wap(self, branch):
        self.wap = branch

    def clear_wap(self):
        self.wap = None

    def refresh(self, r):
        self.refreshed.append(r)

    def enable_wap(self, r):
        self.enabled.append(r)
        self.catalog.tables[r]["properties"]["write.wap.enabled"] = "true"

    def write(self, name: str) -> int:
        r = ref(name)
        wap_on = self.catalog.tables[r]["properties"].get("write.wap.enabled") == "true"
        return self.catalog.write(r, self.wap if (self.wap and wap_on) else "main")
