"""The transaction state machine: begin -> (user writes on the branch) -> commit or rollback.

begin     record each table's main snapshot, create branch <prefix><id> on all of them in one
          atomic catalog commit, refresh Spark's view, set spark.wap.branch.
commit    one multi-table catalog commit that moves every changed table's main to its branch head,
          guarded by "main is still where it was" (+ table uuid, schema id, optional read tables).
rollback  never touch main; drop the branches.
"""

from __future__ import annotations

import logging
import uuid
from enum import Enum
from typing import Iterable

import requests

from .config import CatalogConfig
from .errors import (
    CatalogError,
    CommitFailedError,
    CommitStateUnknownError,
    ConflictError,
    DestructiveSchemaChangeError,
    TransactionUsageError,
    WapNotEnabledError,
)
from .model import TableRef, TableState, current_schema, main_snapshot, ref_snapshot
from .rest import (
    RestCatalogClient,
    assert_ref,
    assert_schema_id,
    assert_uuid,
    remove_ref,
    set_branch,
    table_change,
)
from .schema import destructive_changes
from .spark_ops import SparkOps

log = logging.getLogger("atomic_spark")

DEFAULT_BRANCH_MAX_AGE_MS = 3 * 24 * 3600 * 1000  # orphaned branches expire with snapshot expiry
_BEGIN_ATTEMPTS = 3
_OUTCOME_UNKNOWN_HTTP = {500, 502, 503, 504}
_REF_RETENTION_FIELDS = ("min-snapshots-to-keep", "max-snapshot-age-ms", "max-ref-age-ms")


class State(Enum):
    NEW = "new"
    ACTIVE = "active"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    UNKNOWN = "unknown"


class Transaction:
    def __init__(
        self,
        spark,
        catalog: str,
        tables: Iterable[str],
        *,
        read_tables: Iterable[str] = (),
        enable_wap: bool = False,
        branch_prefix: str = "txn_",
        branch_max_age_ms: int | None = DEFAULT_BRANCH_MAX_AGE_MS,
        client: RestCatalogClient | None = None,
        ops: SparkOps | None = None,
    ):
        if isinstance(tables, str) or isinstance(read_tables, str):
            raise TransactionUsageError("tables and read_tables must be lists of table names, not a string")
        self.catalog = catalog
        self.tables = _parse_unique(tables, catalog)
        self.read_tables = _parse_unique(read_tables, catalog)
        if not self.tables:
            raise TransactionUsageError("tables=[...] must list at least one table the block writes to")
        overlap = set(self.tables) & set(self.read_tables)
        if overlap:
            raise TransactionUsageError(f"Tables listed in both tables and read_tables: {sorted(map(str, overlap))}")
        self.enable_wap = enable_wap
        self.branch_max_age_ms = branch_max_age_ms
        self.id = uuid.uuid4().hex[:16]
        self.branch = f"{branch_prefix}{self.id}"
        self.client = client or RestCatalogClient.from_config(CatalogConfig.from_spark(spark, catalog))
        self.ops = ops or SparkOps(spark, catalog)
        self.state = State.NEW
        self.published: dict[str, int] = {}  # table -> snapshot main now points to (after commit)
        self._states: dict[TableRef, TableState] = {}
        self._read_states: dict[TableRef, tuple[str, int | None]] = {}
        # True once branches were requested, or once the block could run: writes then create
        # branches lazily on tables that had no snapshot at begin.
        self._may_have_branches = False

    def __repr__(self) -> str:
        return f"Transaction(id={self.id!r}, state={self.state.value}, tables={[str(t) for t in self.tables]})"

    # ------------------------------------------------------------------ begin
    def begin(self) -> "Transaction":
        if self.state is not State.NEW:
            raise TransactionUsageError(f"Transaction already {self.state.value}")
        active = self.ops.current_wap()
        if active:
            raise TransactionUsageError(
                f"spark.wap.branch / spark.wap.id is already set ({active!r}). Transactions cannot be "
                "nested, and the session must not be using WAP for anything else."
            )
        try:
            self._ensure_wap_enabled()
            self._create_branches()
            for ref in (*self.tables, *self.read_tables):
                self.ops.refresh(ref)  # Spark caches table metadata; make it see the branch and current main
            self._may_have_branches = True
            self.ops.set_wap(self.branch)
        except BaseException:
            self._cleanup(refresh=False)
            self.state = State.ROLLED_BACK
            raise
        self.state = State.ACTIVE
        log.info("atomic-spark: began %s on %s", self.branch, ", ".join(map(str, self.tables)))
        return self

    def _ensure_wap_enabled(self) -> None:
        missing = [
            ref for ref in self.tables
            if (self.client.load_table(ref).get("properties") or {}).get("write.wap.enabled", "").lower() != "true"
        ]
        if not missing:
            return
        if not self.enable_wap:
            raise WapNotEnabledError([str(r) for r in missing])
        for ref in missing:
            log.info("atomic-spark: enabling write.wap.enabled on %s", ref)
            self.ops.enable_wap(ref)

    def _record_start(self) -> None:
        self._states.clear()
        self._read_states.clear()
        for ref in self.tables:
            md = self.client.load_table(ref)
            start = main_snapshot(md)
            self._states[ref] = TableState(
                ref=ref,
                uuid=md["table-uuid"],
                start_main=start,
                branch_base=start,  # None: no snapshot yet -> branch is created by the first write
                start_schema_id=md["current-schema-id"],
                start_schema=current_schema(md),
            )
        for ref in self.read_tables:
            md = self.client.load_table(ref)
            self._read_states[ref] = (md["table-uuid"], main_snapshot(md))

    def _create_branches(self) -> None:
        for attempt in range(1, _BEGIN_ATTEMPTS + 1):
            self._record_start()
            changes = [
                table_change(
                    s.ref,
                    [assert_uuid(s.uuid), assert_ref("main", s.start_main), assert_ref(self.branch, None)],
                    [set_branch(self.branch, s.start_main, max_ref_age_ms=self.branch_max_age_ms)],
                )
                for s in self._states.values()
                if s.start_main is not None
            ]
            if not changes:
                return
            self._may_have_branches = True
            try:
                self.client.commit_transaction(changes)
                return
            except CatalogError as e:
                if e.status == 409 and attempt < _BEGIN_ATTEMPTS:
                    log.info("atomic-spark: main moved while creating branches, retrying (%s)", e)
                    continue
                if e.status == 409:
                    raise ConflictError(f"Could not create transaction branches: tables kept changing ({e})") from e
                raise

    # ----------------------------------------------------------------- commit
    def commit(self) -> dict[str, int]:
        """Publish every changed table at once. Returns {table: new main snapshot id}."""
        if self.state is not State.ACTIVE:
            raise TransactionUsageError(f"Cannot commit a transaction that is {self.state.value}")
        try:
            self.ops.clear_wap()  # stop routing writes to the branch before reading branch heads
            changes, expected = self._build_commit()
            if expected:
                self._send_commit(changes, expected)
        except CommitStateUnknownError:
            self.state = State.UNKNOWN
            self.ops.clear_wap()
            raise
        except BaseException:
            self._abort()
            raise
        self.state = State.COMMITTED
        self.published = {str(ref): head for ref, (_, head) in expected.items()}
        log.info("atomic-spark: committed %s: %s", self.branch, self.published or "no changes")
        self._cleanup(refresh=True)
        return self.published

    def _build_commit(self) -> tuple[list[dict], dict[TableRef, tuple[int | None, int]]]:
        changes: list[dict] = []
        expected: dict[TableRef, tuple[int | None, int]] = {}
        for ref, s in self._states.items():
            md = self.client.load_table(ref)
            if md["table-uuid"] != s.uuid:
                raise ConflictError(f"{ref} was dropped or replaced during the transaction")
            head = ref_snapshot(md, self.branch)
            if head is None or head == s.branch_base:
                continue  # table not written in this transaction
            schema_id = md["current-schema-id"]
            if schema_id != s.start_schema_id:
                bad = destructive_changes(s.start_schema, current_schema(md))
                if bad:
                    raise DestructiveSchemaChangeError(str(ref), bad)
            main_ref = (md.get("refs") or {}).get("main") or {}
            retention = {k: main_ref[k] for k in _REF_RETENTION_FIELDS if k in main_ref}
            changes.append(table_change(
                ref,
                [assert_uuid(s.uuid), assert_ref("main", s.start_main), assert_schema_id(schema_id)],
                [{**set_branch("main", head), **retention}],
            ))
            expected[ref] = (s.start_main, head)
        if expected:
            for ref, (table_uuid, main) in self._read_states.items():
                changes.append(table_change(ref, [assert_uuid(table_uuid), assert_ref("main", main)], []))
        return changes, expected

    def _send_commit(self, changes: list[dict], expected: dict[TableRef, tuple[int | None, int]]) -> None:
        try:
            self.client.commit_transaction(changes)
            return
        except CatalogError as e:
            if e.status == 409:
                raise ConflictError(f"Another writer changed a table during the transaction; nothing was published ({e})") from e
            if e.status not in _OUTCOME_UNKNOWN_HTTP:
                raise CommitFailedError(f"Catalog rejected the commit; nothing was published ({e})") from e
            cause: Exception = e
        except requests.RequestException as e:
            cause = e
        self._resolve_unknown(expected, cause)

    def _resolve_unknown(self, expected: dict[TableRef, tuple[int | None, int]], cause: Exception) -> None:
        """The commit response was lost or ambiguous: look at the tables to find out what happened."""
        detail = {str(r): {"start": start, "branch_head": head} for r, (start, head) in expected.items()}
        try:
            observed = {ref: main_snapshot(self.client.load_table(ref)) for ref in expected}
        except Exception as e:  # noqa: BLE001
            raise CommitStateUnknownError(
                f"Commit outcome unknown ({cause}), and the tables could not be re-read ({e}).", self.branch, detail
            ) from cause
        if all(observed[r] == head for r, (_, head) in expected.items()):
            log.warning("atomic-spark: commit response was lost (%s) but every table is published", cause)
            return
        if all(observed[r] == start for r, (start, _) in expected.items()):
            raise CommitFailedError(f"Commit failed ({cause}); verified that no table was changed") from cause
        for r in expected:
            detail[str(r)]["main_now"] = observed[r]
        raise CommitStateUnknownError("Commit outcome unknown: tables are in a mixed state.", self.branch, detail) from cause

    # --------------------------------------------------------------- rollback
    def rollback(self) -> None:
        if self.state is State.ACTIVE:
            self._abort()
        elif self.state is not State.NEW:
            raise TransactionUsageError(f"Cannot roll back a transaction that is {self.state.value}")

    def _abort(self) -> None:
        self._cleanup(refresh=True)
        self.state = State.ROLLED_BACK
        log.info("atomic-spark: rolled back %s", self.branch)

    # ---------------------------------------------------------------- cleanup
    def _cleanup(self, refresh: bool) -> None:
        """Best effort: never raises, so it cannot mask the error that caused it."""
        try:
            self.ops.clear_wap()
        except Exception:  # noqa: BLE001
            log.exception("atomic-spark: could not unset spark.wap.branch")
        if self._may_have_branches:
            self._drop_branches()
        if refresh:
            for ref in (*self.tables, *self.read_tables):
                try:
                    self.ops.refresh(ref)
                except Exception as e:  # noqa: BLE001
                    log.warning("atomic-spark: could not refresh %s: %s", ref, e)

    def _drop_branches(self) -> None:
        refs = list(self.tables)
        try:
            self.client.commit_transaction([table_change(r, [], [remove_ref(self.branch)]) for r in refs])
            return
        except Exception as e:  # noqa: BLE001
            log.warning("atomic-spark: dropping branch %s in one commit failed (%s); retrying per table", self.branch, e)
        for ref in refs:
            try:
                self.client.commit_table(ref, [], [remove_ref(self.branch)])
            except Exception as e:  # noqa: BLE001
                log.warning("atomic-spark: could not drop branch %s on %s: %s", self.branch, ref, e)


def _parse_unique(names: Iterable[str], catalog: str) -> list[TableRef]:
    out: list[TableRef] = []
    for n in names:
        ref = TableRef.parse(n, catalog)
        if ref not in out:
            out.append(ref)
    return out
