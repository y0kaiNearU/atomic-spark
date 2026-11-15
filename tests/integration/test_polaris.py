"""End-to-end tests against the docker compose stack (Polaris + Spark + Iceberg).

    docker compose up -d --wait polaris-setup
    docker compose run --rm spark pytest tests/integration -v

Fixtures are in conftest.py; failure injection is in test_failures.py.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from atomic_spark import (  # noqa: E402
    ConflictError,
    DestructiveSchemaChangeError,
    State,
    WapNotEnabledError,
    atomic,
)
from atomic_spark.rest import RestCatalogClient  # noqa: E402

from .helpers import CAT, refs, rows  # noqa: E402

pytestmark = pytest.mark.integration


def test_client_selection(spark, monkeypatch, client_kind):
    from atomic_spark.clients import make_client
    from atomic_spark.jvm import JvmCatalogClient

    monkeypatch.delenv("ATOMIC_SPARK_CLIENT")
    assert isinstance(make_client(spark, CAT), JvmCatalogClient)  # classic session -> JVM path by default
    assert isinstance(make_client(spark, CAT, "http"), RestCatalogClient)


def test_commit_publishes_all_write_types_atomically(spark, rest, make):
    a, b, c = make(), make(), make()
    with atomic(spark, CAT, tables=[a, b, c]) as tx:
        spark.sql(f"MERGE INTO {CAT}.{a} t USING (SELECT 1 id, 'm' v UNION ALL SELECT 9, 'n') s ON t.id = s.id "
                  "WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT *")
        spark.sql(f"DELETE FROM {CAT}.{b} WHERE id = 2")
        spark.createDataFrame([(7, "df")], "id BIGINT, v STRING").writeTo(f"{CAT}.{c}").append()
        # read-your-writes inside the block
        assert rows(spark, a) == 4 and rows(spark, b) == 2 and rows(spark, c) == 4
        # not visible to anyone else yet
        other = spark.newSession()
        assert other.sql(f"SELECT count(*) FROM {CAT}.{a}").collect()[0][0] == 3
    assert tx.state is State.COMMITTED and set(tx.published) == {a, b, c}
    # same session sees the published data immediately (tables were refreshed)
    assert rows(spark, a) == 4 and rows(spark, a, "v = 'm'") == 1
    assert rows(spark, b) == 2 and rows(spark, c) == 4
    assert refs(rest, a) == refs(rest, b) == refs(rest, c) == ["main"]


def test_exception_rolls_back_every_table(spark, rest, make):
    a, b = make(), make()
    with pytest.raises(RuntimeError, match="stage 3 failed"):
        with atomic(spark, CAT, tables=[a, b]) as tx:
            spark.sql(f"DELETE FROM {CAT}.{a}")
            spark.sql(f"INSERT INTO {CAT}.{b} VALUES (4,'d')")
            raise RuntimeError("stage 3 failed")
    assert tx.state is State.ROLLED_BACK
    assert rows(spark, a) == 3 and rows(spark, b) == 3
    assert refs(rest, a) == refs(rest, b) == ["main"]
    assert spark.conf.get("spark.wap.branch", None) is None


def test_failing_spark_action_rolls_back(spark, make):
    a = make()
    with pytest.raises(Exception, match="(?i)cannot be found|not found"):
        with atomic(spark, CAT, tables=[a]):
            spark.sql(f"INSERT INTO {CAT}.{a} VALUES (4,'d')")
            spark.sql(f"SELECT * FROM {CAT}.no_such_ns.no_such_table").show()
    assert rows(spark, a) == 3


def test_concurrent_writer_conflict(spark, make):
    a, b = make(), make()
    with pytest.raises(ConflictError):
        with atomic(spark, CAT, tables=[a, b]):
            spark.sql(f"INSERT INTO {CAT}.{a} VALUES (4,'d')")
            spark.sql(f"INSERT INTO {CAT}.{b} VALUES (4,'d')")
            spark.newSession().sql(f"INSERT INTO {CAT}.{a} VALUES (100,'other')")
    assert rows(spark, a) == 4 and rows(spark, a, "id = 100") == 1  # only the other writer's row
    assert rows(spark, b) == 3


def test_decorator_retries_after_conflict(spark, make):
    a = make()
    attempts = []

    @atomic(spark, CAT, tables=[a], retries=2, retry_wait=0.1)
    def job():
        attempts.append(1)
        spark.sql(f"INSERT INTO {CAT}.{a} VALUES ({10 + len(attempts)},'job')")
        if len(attempts) == 1:
            spark.newSession().sql(f"INSERT INTO {CAT}.{a} VALUES (100,'other')")

    job()
    assert len(attempts) == 2
    assert rows(spark, a) == 5 and rows(spark, a, "v = 'job'") == 1


def test_empty_tables_written_and_untouched(spark, rest, make):
    written, untouched = make(rows=False), make(rows=False)
    with atomic(spark, CAT, tables=[written, untouched]) as tx:
        spark.sql(f"MERGE INTO {CAT}.{written} t USING (SELECT 1 id, 'x' v) s ON t.id = s.id "
                  "WHEN NOT MATCHED THEN INSERT *")
    assert tx.published.keys() == {written}
    assert rows(spark, written) == 1 and rows(spark, untouched) == 0
    assert refs(rest, written) == ["main"] and refs(rest, untouched) == []


def test_read_table_validation(spark, make):
    a, src = make(), make()
    with pytest.raises(ConflictError):
        with atomic(spark, CAT, tables=[a], read_tables=[src]):
            spark.sql(f"INSERT INTO {CAT}.{a} SELECT * FROM {CAT}.{src}")
            spark.newSession().sql(f"INSERT INTO {CAT}.{src} VALUES (4,'late')")
    assert rows(spark, a) == 3


def test_wap_not_enabled_is_rejected_and_can_be_enabled(spark, make):
    t = make(wap=False)
    with pytest.raises(WapNotEnabledError):
        with atomic(spark, CAT, tables=[t]):
            pass
    with atomic(spark, CAT, tables=[t], enable_wap=True):
        spark.sql(f"INSERT INTO {CAT}.{t} VALUES (4,'d')")
    assert rows(spark, t) == 4


def test_additive_ddl_allowed_destructive_ddl_fails(spark, make):
    a = make()
    with atomic(spark, CAT, tables=[a]):
        spark.sql(f"ALTER TABLE {CAT}.{a} ADD COLUMN extra STRING")
        spark.sql(f"INSERT INTO {CAT}.{a} VALUES (4,'d','x')")
    assert rows(spark, a, "extra = 'x'") == 1

    b = make()
    with pytest.raises(DestructiveSchemaChangeError, match="dropped column v"):
        with atomic(spark, CAT, tables=[b]):
            spark.sql(f"INSERT INTO {CAT}.{b} VALUES (4,'d')")
            spark.sql(f"ALTER TABLE {CAT}.{b} DROP COLUMN v")
    assert rows(spark, b) == 3


def test_sequential_blocks(spark, make):
    a = make()
    with atomic(spark, CAT, tables=[a]):
        spark.sql(f"INSERT INTO {CAT}.{a} VALUES (4,'d')")
    with pytest.raises(ValueError):
        with atomic(spark, CAT, tables=[a]):
            spark.sql(f"INSERT INTO {CAT}.{a} VALUES (5,'e')")
            raise ValueError
    with atomic(spark, CAT, tables=[a]):
        spark.sql(f"INSERT INTO {CAT}.{a} VALUES (6,'f')")
    assert rows(spark, a) == 5 and rows(spark, a, "id = 5") == 0
