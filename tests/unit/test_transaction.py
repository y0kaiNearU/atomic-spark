from __future__ import annotations

import contextlib

import pytest
import requests

from atomic_spark import (
    CatalogError,
    CommitFailedError,
    CommitStateUnknownError,
    ConflictError,
    DestructiveSchemaChangeError,
    State,
    TransactionUsageError,
    WapNotEnabledError,
    atomic,
)

from .fakes import BASE_SCHEMA, CATALOG, FakeCatalog, FakeOps, ref


@pytest.fixture
def cat():
    c = FakeCatalog()
    for t in ("db.a", "db.b", "db.c"):
        c.create(t)
    return c


@pytest.fixture
def ops(cat):
    return FakeOps(cat)


def tx(cat, ops, tables, **kw):
    return atomic(None, CATALOG, tables, client=cat, ops=ops, **kw)


def branches(cat, name):
    return [b for b in cat.refs(name) if b != "main"]


# ------------------------------------------------------------------ happy path


def test_commit_publishes_every_written_table_at_once(cat, ops):
    a0, b0, c0 = cat.main("db.a"), cat.main("db.b"), cat.main("db.c")
    with tx(cat, ops, ["db.a", "db.b", "db.c"]) as t:
        assert ops.wap == t.branch
        ha, hb = ops.write("db.a"), ops.write("db.b")
        ha = ops.write("db.a")  # several writes to one table
        assert (cat.main("db.a"), cat.main("db.b")) == (a0, b0)  # nothing visible yet
    assert t.state is State.COMMITTED
    assert (cat.main("db.a"), cat.main("db.b"), cat.main("db.c")) == (ha, hb, c0)
    assert t.published == {"db.a": ha, "db.b": hb}
    assert ops.wap is None
    assert all(branches(cat, n) == [] for n in ("db.a", "db.b", "db.c"))
    # the publish was a single multi-table commit
    publish = [c for c in cat.commits if any(u.get("ref-name") == "main" for ch in c for u in ch["updates"])]
    assert len(publish) == 1 and len(publish[0]) == 2


def test_tables_are_refreshed_after_commit(cat, ops):
    with tx(cat, ops, ["db.a"]):
        ops.write("db.a")
        ops.refreshed.clear()
    assert ref("db.a") in ops.refreshed


def test_no_writes_means_no_publish(cat, ops):
    a0 = cat.main("db.a")
    with tx(cat, ops, ["db.a"]) as t:
        pass
    assert t.state is State.COMMITTED and t.published == {} and cat.main("db.a") == a0


def test_catalog_prefix_in_table_names_is_accepted(cat, ops):
    with tx(cat, ops, [f"{CATALOG}.db.a"]) as t:
        ops.write("db.a")
    assert t.published.keys() == {"db.a"}


def test_main_retention_settings_are_preserved(cat, ops):
    cat.tables[ref("db.a")]["refs"]["main"]["min-snapshots-to-keep"] = 5
    with tx(cat, ops, ["db.a"]):
        ops.write("db.a")
    assert cat.tables[ref("db.a")]["refs"]["main"]["min-snapshots-to-keep"] == 5


def test_transaction_branch_gets_max_ref_age(cat, ops):
    with tx(cat, ops, ["db.a"]) as t:
        assert cat.tables[ref("db.a")]["refs"][t.branch]["max-ref-age-ms"] > 0


# -------------------------------------------------------------------- rollback


def test_exception_in_block_publishes_nothing(cat, ops):
    a0, b0 = cat.main("db.a"), cat.main("db.b")
    with pytest.raises(ZeroDivisionError):
        with tx(cat, ops, ["db.a", "db.b"]) as t:
            ops.write("db.a")
            ops.write("db.b")
            1 / 0
    assert t.state is State.ROLLED_BACK
    assert (cat.main("db.a"), cat.main("db.b")) == (a0, b0)
    assert branches(cat, "db.a") == branches(cat, "db.b") == []
    assert ops.wap is None


def test_keyboard_interrupt_also_rolls_back(cat, ops):
    a0 = cat.main("db.a")
    with pytest.raises(KeyboardInterrupt):
        with tx(cat, ops, ["db.a"]):
            ops.write("db.a")
            raise KeyboardInterrupt
    assert cat.main("db.a") == a0 and branches(cat, "db.a") == []


def test_rollback_survives_failing_branch_cleanup(cat, ops):
    with pytest.raises(ValueError, match="user error"):
        with tx(cat, ops, ["db.a"]) as t:
            ops.write("db.a")
            cat.failures = [("before", CatalogError(500, "X", "boom"))]  # the multi-table cleanup commit fails
            raise ValueError("user error")
    assert t.state is State.ROLLED_BACK
    assert branches(cat, "db.a") == []  # per-table fallback dropped it


# ------------------------------------------------------------------- conflicts


def test_concurrent_writer_causes_conflict_and_nothing_is_published(cat, ops):
    b0 = cat.main("db.b")
    with pytest.raises(ConflictError):
        with tx(cat, ops, ["db.a", "db.b"]) as t:
            ops.write("db.a")
            ops.write("db.b")
            cat.write(ref("db.a"), "main")  # someone else commits to main
    other = cat.main("db.a")
    assert cat.main("db.b") == b0 and cat.main("db.a") == other
    assert t.state is State.ROLLED_BACK and branches(cat, "db.a") == []


def test_read_table_change_causes_conflict(cat, ops):
    a0 = cat.main("db.a")
    with pytest.raises(ConflictError):
        with tx(cat, ops, ["db.a"], read_tables=["db.c"]):
            ops.write("db.a")
            cat.write(ref("db.c"), "main")
    assert cat.main("db.a") == a0


def test_unchanged_read_table_does_not_block(cat, ops):
    with tx(cat, ops, ["db.a"], read_tables=["db.c"]) as t:
        ops.write("db.a")
    assert t.state is State.COMMITTED


def test_dropped_and_recreated_table_is_a_conflict(cat, ops):
    with pytest.raises(ConflictError, match="dropped or replaced"):
        with tx(cat, ops, ["db.a"]):
            ops.write("db.a")
            cat.tables[ref("db.a")]["table-uuid"] = "recreated"


def test_decorator_retries_on_conflict_then_succeeds(cat, ops, monkeypatch):
    monkeypatch.setattr("atomic_spark.api.time.sleep", lambda s: None)
    calls = []

    @tx(cat, ops, ["db.a"], retries=2, retry_wait=0)
    def job():
        calls.append(1)
        ops.write("db.a")
        if len(calls) == 1:
            cat.write(ref("db.a"), "main")  # concurrent writer, first attempt only
        return "done"

    assert job() == "done"
    assert len(calls) == 2
    assert branches(cat, "db.a") == []


def test_decorator_gives_up_after_retries(cat, ops, monkeypatch):
    monkeypatch.setattr("atomic_spark.api.time.sleep", lambda s: None)

    @tx(cat, ops, ["db.a"], retries=1)
    def job():
        ops.write("db.a")
        cat.write(ref("db.a"), "main")

    with pytest.raises(ConflictError):
        job()


def test_decorator_does_not_retry_user_errors(cat, ops):
    calls = []

    @tx(cat, ops, ["db.a"], retries=3)
    def job():
        calls.append(1)
        raise RuntimeError("bug")

    with pytest.raises(RuntimeError):
        job()
    assert len(calls) == 1


def test_failed_branch_creation_cleans_up_and_never_runs_block(cat, ops):
    cat.failures = [("after", CatalogError(503, "X", "lost"))]  # branches created, response lost
    with pytest.raises(CatalogError):
        with tx(cat, ops, ["db.a", "db.b"]):
            pytest.fail("block must not run")
    assert branches(cat, "db.a") == branches(cat, "db.b") == [] and ops.wap is None


def test_begin_retries_when_main_moves_while_creating_branches(cat, ops):
    original = cat.commit_transaction
    state = {"n": 0}

    def flaky(changes):
        state["n"] += 1
        if state["n"] == 1:
            cat.write(ref("db.a"), "main")  # main moves between load and branch creation
        return original(changes)

    cat.commit_transaction = flaky
    with tx(cat, ops, ["db.a"]) as t:
        assert cat.refs("db.a")[t.branch] == cat.main("db.a")


# ------------------------------------------------------------- WAP / usage


def test_table_without_wap_is_rejected_before_anything_happens(cat, ops):
    cat.create("db.nowap", wap=False)
    with pytest.raises(WapNotEnabledError, match="db.nowap"):
        with tx(cat, ops, ["db.a", "db.nowap"]):
            pytest.fail("block must not run")
    assert branches(cat, "db.a") == [] and ops.wap is None and cat.commits == []


def test_enable_wap_sets_the_property(cat, ops):
    cat.create("db.nowap", wap=False)
    with tx(cat, ops, ["db.nowap"], enable_wap=True) as t:
        ops.write("db.nowap")
    assert ops.enabled == [ref("db.nowap")] and t.published.keys() == {"db.nowap"}


def test_nesting_is_rejected(cat, ops):
    with tx(cat, ops, ["db.a"]):
        with pytest.raises(TransactionUsageError, match="nested"):
            with tx(cat, ops, ["db.b"]):
                pass


def test_sequential_blocks_work(cat, ops):
    with tx(cat, ops, ["db.a"]) as t1:
        ops.write("db.a")
    with tx(cat, ops, ["db.b"]) as t2:
        ops.write("db.b")
    assert t1.state is t2.state is State.COMMITTED and t1.branch != t2.branch


def test_retries_rejected_for_with_block(cat, ops):
    with pytest.raises(TransactionUsageError, match="decorator"):
        with tx(cat, ops, ["db.a"], retries=2):
            pass


@pytest.mark.parametrize("tables", [[], "db.a", ["a"]])
def test_bad_table_arguments(cat, ops, tables):
    with pytest.raises(TransactionUsageError):
        with tx(cat, ops, tables):
            pass


def test_overlapping_tables_and_read_tables(cat, ops):
    with pytest.raises(TransactionUsageError, match="both"):
        with tx(cat, ops, ["db.a"], read_tables=["db.a"]):
            pass


# ----------------------------------------------------------------- empty tables


def test_empty_table_branch_is_created_by_first_write(cat, ops):
    cat.create("db.empty", rows=False)
    with tx(cat, ops, ["db.empty", "db.a"]) as t:
        assert t.branch not in cat.refs("db.empty")
        head = ops.write("db.empty")
    assert cat.main("db.empty") == head and branches(cat, "db.empty") == []


@pytest.mark.parametrize("fail", [False, True])
def test_only_empty_tables_branches_are_still_dropped(cat, ops, fail):
    cat.create("db.e1", rows=False)
    cat.create("db.e2", rows=False)
    with pytest.raises(ValueError) if fail else contextlib.nullcontext():
        with tx(cat, ops, ["db.e1", "db.e2"]):
            ops.write("db.e1")
            if fail:
                raise ValueError
    assert branches(cat, "db.e1") == [] and branches(cat, "db.e2") == []
    assert (cat.main("db.e1") is None) == fail


def test_empty_table_conflict_when_someone_else_writes_first(cat, ops):
    cat.create("db.empty", rows=False)
    with pytest.raises(ConflictError):
        with tx(cat, ops, ["db.empty"]):
            ops.write("db.empty")
            cat.write(ref("db.empty"), "main")


# ---------------------------------------------------------------------- schema


def test_additive_schema_change_is_allowed(cat, ops):
    with tx(cat, ops, ["db.a"]) as t:
        cat.evolve_schema(ref("db.a"), BASE_SCHEMA["fields"] + [{"id": 3, "name": "extra", "required": False, "type": "string"}])
        ops.write("db.a")
    assert t.state is State.COMMITTED


def test_destructive_schema_change_fails_the_commit(cat, ops):
    a0 = cat.main("db.a")
    with pytest.raises(DestructiveSchemaChangeError, match="dropped column v"):
        with tx(cat, ops, ["db.a"]):
            ops.write("db.a")
            cat.evolve_schema(ref("db.a"), BASE_SCHEMA["fields"][:1])
    assert cat.main("db.a") == a0 and branches(cat, "db.a") == []


# ------------------------------------------------------------- commit outcomes


def _run_with_publish_failure(cat, ops, when, exc):
    def body():
        ops.write("db.a")
        ops.write("db.b")
        cat.failures = [(when, exc)]

    t = tx(cat, ops, ["db.a", "db.b"])
    with t as transaction:
        body()
    return transaction


def test_lost_response_but_applied_counts_as_committed(cat, ops):
    t = _run_with_publish_failure(cat, ops, "after", requests.ConnectionError("reset"))
    assert t.state is State.COMMITTED
    assert cat.main("db.a") == t.published["db.a"]


def test_5xx_not_applied_is_a_definite_failure(cat, ops):
    a0 = cat.main("db.a")
    with pytest.raises(CommitFailedError, match="verified"):
        _run_with_publish_failure(cat, ops, "before", CatalogError(503, "ServiceUnavailable", "down"))
    assert cat.main("db.a") == a0 and branches(cat, "db.a") == []


def test_4xx_is_a_definite_failure(cat, ops):
    with pytest.raises(CommitFailedError):
        _run_with_publish_failure(cat, ops, "before", CatalogError(403, "ForbiddenException", "no"))


def test_mixed_state_keeps_branches_for_repair(cat, ops):
    def half_apply(changes):
        # pathological server: applies only the first table, then times out
        first = changes[0]
        r = ref(".".join(first["identifier"]["namespace"] + [first["identifier"]["name"]]))
        cat.tables[r]["refs"]["main"] = {"snapshot-id": first["updates"][0]["snapshot-id"], "type": "branch"}
        raise requests.Timeout("timed out")

    with pytest.raises(CommitStateUnknownError) as ei:
        with tx(cat, ops, ["db.a", "db.b"]) as t:
            ops.write("db.a")
            ops.write("db.b")
            original = cat.commit_transaction
            cat.commit_transaction = lambda ch: half_apply(ch) if any(
                u.get("ref-name") == "main" for c in ch for u in c["updates"]) else original(ch)
    assert t.state is State.UNKNOWN
    assert t.branch in cat.refs("db.a") and t.branch in cat.refs("db.b")
    assert ei.value.branch == t.branch and ops.wap is None
