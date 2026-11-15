"""Failure injection against real Polaris: lost or late commit responses (FaultProxy) and a killed driver.

The rule under test: whatever atomic-spark reports must match what the catalog ends up with, even
when the publish commit's response never arrives.
"""

from __future__ import annotations

import json
import os
import queue
import signal
import subprocess
import sys
import threading
import time

import pytest

pytest.importorskip("pyspark")

from atomic_spark import CommitFailedError, State, atomic  # noqa: E402
from atomic_spark.model import TableRef  # noqa: E402

from .helpers import CAT, main_id, refs, rows  # noqa: E402

pytestmark = pytest.mark.integration

TIMEOUT_KEY = f"spark.sql.catalog.{CAT}.rest.client.socket-timeout-ms"


def write_both(spark, a, b):
    spark.sql(f"INSERT INTO {CAT}.{a} VALUES (4,'d')")
    spark.sql(f"DELETE FROM {CAT}.{b} WHERE id = 1")


def assert_untouched(spark, rest, starts):
    for t, start in starts.items():
        assert main_id(rest, t) == start
        assert rows(spark, t) == 3
        assert refs(rest, t) == ["main"]


def assert_published(spark, rest, tx, a, b):
    assert tx.state is State.COMMITTED and set(tx.published) == {a, b}
    assert main_id(rest, a) == tx.published[a] and main_id(rest, b) == tx.published[b]
    assert rows(spark, a) == 4 and rows(spark, b) == 2
    assert refs(rest, a) == refs(rest, b) == ["main"]


@pytest.fixture
def short_timeout(spark):
    """A 3 s socket timeout for the catalog clients atomic-spark builds (both read this option).
    Spark's own catalog was built at session start and keeps its default."""
    spark.conf.set(TIMEOUT_KEY, "3000")
    yield 3.0
    spark.conf.unset(TIMEOUT_KEY)


def test_commit_that_never_reaches_the_catalog_fails_cleanly(spark, rest, make, proxy):
    a, b = make(), make()
    starts = {a: main_id(rest, a), b: main_id(rest, b)}
    proxy.arm("drop", times=10)
    with pytest.raises(CommitFailedError, match="verified"):
        with atomic(spark, CAT, tables=[a, b]) as tx:
            write_both(spark, a, b)
    assert tx.state is State.ROLLED_BACK
    assert proxy.publish_requests == 1  # neither client retries a commit whose request may have been sent
    proxy.disarm()
    assert_untouched(spark, rest, starts)
    with atomic(spark, CAT, tables=[a, b]) as tx:  # the tables are usable straight away
        write_both(spark, a, b)
    assert_published(spark, rest, tx, a, b)


@pytest.mark.parametrize("fault", ["apply_then_drop", "apply_then_504"])
def test_commit_applied_but_response_lost_counts_as_committed(spark, rest, make, proxy, fault):
    a, b = make(), make()
    proxy.arm(fault)
    with atomic(spark, CAT, tables=[a, b]) as tx:
        write_both(spark, a, b)
    assert proxy.publish_requests == 1
    assert_published(spark, rest, tx, a, b)


def test_timed_out_commit_still_in_flight_can_never_apply(spark, rest, make, proxy, short_timeout):
    """The request is stuck while the client times out, checks, and reports failure. When it finally
    reaches Polaris it must be rejected; otherwise main would move after the job was told it didn't."""
    a, b = make(), make()
    starts = {a: main_id(rest, a), b: main_id(rest, b)}
    proxy.arm("hold")
    began = time.monotonic()
    with pytest.raises(CommitFailedError, match="verified"):
        with atomic(spark, CAT, tables=[a, b]):
            write_both(spark, a, b)
    assert time.monotonic() - began >= short_timeout  # it really was the client timeout that fired
    assert proxy.publish_requests == 1
    late = proxy.release()
    assert late.status == 409, late.body  # fenced: its branch assertion fails
    assert_untouched(spark, rest, starts)


def test_killed_driver_leaves_main_untouched_and_an_expiring_branch(spark, spark_conf, rest, make, client_kind,
                                                                   tmp_path):
    t = make()
    start = main_id(rest, t)
    script = os.path.join(os.path.dirname(__file__), "killed_driver.py")
    env = {**os.environ, "ATOMIC_IT_SPARK_CONF": json.dumps(spark_conf), "ATOMIC_SPARK_CLIENT": client_kind}
    log = tmp_path / "driver.log"
    with open(log, "w") as err:  # a file, not a pipe: Spark's stderr would fill an unread pipe and hang
        proc = subprocess.Popen([sys.executable, script, CAT, t], env=env, stdout=subprocess.PIPE,
                                stderr=err, text=True, start_new_session=True)
    lines: queue.Queue[str] = queue.Queue()
    threading.Thread(target=lambda: [lines.put(line) for line in proc.stdout], daemon=True).start()
    try:
        line = lines.get(timeout=180)
    except queue.Empty:
        os.killpg(proc.pid, signal.SIGKILL)
        pytest.fail(f"driver never reported its write; stderr:\n{proc.stderr.read()[-3000:]}")
    assert line.startswith("WRITTEN "), line
    branch = line.split()[1]
    os.killpg(proc.pid, signal.SIGKILL)  # the driver and its JVM, mid-block
    proc.wait(30)

    spark.sql(f"REFRESH TABLE {CAT}.{t}")
    assert main_id(rest, t) == start and rows(spark, t) == 3 and rows(spark, t, "id = 99") == 0
    md = rest.load_table(TableRef.parse(t, CAT))
    orphan = md["refs"][branch]
    assert orphan["snapshot-id"] != start  # holds the killed driver's unpublished write
    assert orphan["max-ref-age-ms"] == 3 * 24 * 3600 * 1000  # snapshot expiry removes it eventually

    with atomic(spark, CAT, tables=[t]) as tx:  # an orphaned branch does not block later blocks
        spark.sql(f"INSERT INTO {CAT}.{t} VALUES (4,'d')")
    assert tx.state is State.COMMITTED and rows(spark, t) == 4 and rows(spark, t, "id = 99") == 0
    assert refs(rest, t) == sorted(["main", branch])  # still there until gc (M3) or expiry
