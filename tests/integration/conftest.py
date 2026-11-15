"""Fixtures for the integration tests: a Spark session whose catalog traffic goes through FaultProxy."""

from __future__ import annotations

import os
import time
import uuid
from urllib.parse import urlsplit

import pytest

from .faultproxy import FaultProxy
from .helpers import CAT


def _auth_conf() -> dict[str, str]:
    """POLARIS_AUTH=polaris (default): Polaris's built-in OAuth, realm POLARIS.
    POLARIS_AUTH=keycloak: tokens from Keycloak only, realm EXTERNAL (docker-compose.keycloak.yml)."""
    if os.environ.get("POLARIS_AUTH", "polaris") == "keycloak":
        return {
            "credential": "client1:s3cr3t",
            "oauth2-server-uri": "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token",
            "scope": "profile",
            "header.Polaris-Realm": "EXTERNAL",
        }
    return {
        "credential": os.environ.get("POLARIS_CREDENTIAL", "root:s3cr3t"),
        "scope": "PRINCIPAL_ROLE:ALL",
        "header.Polaris-Realm": "POLARIS",
    }


@pytest.fixture(scope="session")
def proxy():
    """Every catalog request (Spark's and atomic-spark's) goes through this; see faultproxy.py."""
    uri = urlsplit(os.environ.get("POLARIS_URI", "http://polaris:8181/api/catalog"))
    p = FaultProxy(f"{uri.scheme}://{uri.netloc}").start()
    p.catalog_uri = f"{p.url}{uri.path}"
    yield p
    p.stop()


@pytest.fixture(autouse=True)
def _disarm(request):
    yield
    if "proxy" in request.fixturenames:
        request.getfixturevalue("proxy").disarm()


@pytest.fixture(scope="session")
def spark_conf(proxy) -> dict[str, str]:
    conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{CAT}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CAT}.type": "rest",
        f"spark.sql.catalog.{CAT}.uri": proxy.catalog_uri,
        f"spark.sql.catalog.{CAT}.warehouse": os.environ.get("POLARIS_WAREHOUSE", "atomic"),
        "spark.hadoop.fs.permissions.umask-mode": "000",
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": "2",
    }
    conf.update({f"spark.sql.catalog.{CAT}.{k}": v for k, v in _auth_conf().items()})
    return conf


@pytest.fixture(scope="session")
def spark(spark_conf):
    from pyspark.sql import SparkSession

    b = SparkSession.builder.master("local[2]").appName("atomic-spark-it")
    for k, v in spark_conf.items():
        b = b.config(k, v)
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(autouse=True, params=["jvm", "http"])
def client_kind(request, monkeypatch):
    """Run every test once through Spark's JVM Iceberg client and once through the Python HTTP client."""
    monkeypatch.setenv("ATOMIC_SPARK_CLIENT", request.param)
    return request.param


@pytest.fixture(scope="session")
def ns(spark):
    name = f"it_{int(time.time())}_{uuid.uuid4().hex[:4]}"
    spark.sql(f"CREATE NAMESPACE {CAT}.{name}")
    return name


@pytest.fixture(scope="session")
def rest(spark):
    from atomic_spark.config import CatalogConfig
    from atomic_spark.rest import RestCatalogClient

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
