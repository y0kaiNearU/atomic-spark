"""End-to-end tests against the docker compose stack (Polaris + Spark + Iceberg).

    docker compose up -d --wait polaris-setup
    docker compose run --rm spark pytest tests/integration -v
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from atomic_spark import (  # noqa: E402
    ConflictError,
    DestructiveSchemaChangeError,
    State,
    WapNotEnabledError,
    atomic,
)
from atomic_spark.config import CatalogConfig  # noqa: E402
from atomic_spark.model import TableRef  # noqa: E402
from atomic_spark.rest import RestCatalogClient  # noqa: E402

pytestmark = pytest.mark.integration
CAT = "polaris"


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("atomic-spark-it")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CAT}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CAT}.type", "rest")
        .config(f"spark.sql.catalog.{CAT}.uri", os.environ.get("POLARIS_URI", "http://polaris:8181/api/catalog"))
        .config(f"spark.sql.catalog.{CAT}.warehouse", os.environ.get("POLARIS_WAREHOUSE", "atomic"))
        .config(f"spark.sql.catalog.{CAT}.credential", os.environ.get("POLARIS_CREDENTIAL", "root:s3cr3t"))
        .config(f"spark.sql.catalog.{CAT}.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def ns(spark):
    name = f"it_{int(time.time())}_{uuid.uuid4().hex[:4]}"
    spark.sql(f"CREATE NAMESPACE {CAT}.{name}")
    return name


@pytest.fixture(scope="session")
def rest(spark):
    return RestCatalogClient.from_config(CatalogConfig.from_spark(spark, CAT))


@pytest.fixture
def make(spark, ns):
    def _make(rows: bool = True, wap: bool = True) -> str:
        t = f"{ns}.t_{uuid.uuid4().hex[:8]}"
        props = "'format-version'='2'" + (", 'write.wap.enabled'='true'" if wap else "")
        spark.sql(f"CREATE TABLE {CAT}.{t} (id BIGINT, v STRING) USING iceberg TBLPROPERTIES ({props})")
        if rows:
            spark.sql(f"INSERT INTO {CAT}.{t} VALUES (1,'a'),(2,'b'),(3,'c')")
        return t

    return _make


def rows(spark, t, where="true"):
    return spark.sql(f"SELECT count(*) FROM {CAT}.{t} WHERE {where}").collect()[0][0]


def refs(rest, t):
    md = rest.load_table(TableRef.parse(t, CAT))
    return sorted((md.get("refs") or {}).keys())


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
