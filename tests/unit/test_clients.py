from __future__ import annotations

import pytest

from atomic_spark import CatalogError, State, TransactionUsageError, atomic
from atomic_spark.clients import make_client
from atomic_spark.jvm import _to_catalog_error, jvm_available
from atomic_spark.rest import RestCatalogClient

from .fakes import CATALOG, FakeCatalog, FakeOps, ref
from .test_rest_and_config import BASE, FakeSpark


def test_auto_without_jvm_uses_http(monkeypatch):
    monkeypatch.delenv("ATOMIC_SPARK_CLIENT", raising=False)
    spark = FakeSpark(BASE)  # like Spark Connect: no _jvm
    assert not jvm_available(spark)
    assert isinstance(make_client(spark, "polaris"), RestCatalogClient)


def test_env_var_overrides_auto(monkeypatch):
    monkeypatch.setenv("ATOMIC_SPARK_CLIENT", "jvm")
    with pytest.raises(TransactionUsageError, match="Spark Connect"):
        make_client(FakeSpark(BASE), "polaris")


def test_explicit_jvm_without_jvm_is_a_usage_error():
    with pytest.raises(TransactionUsageError, match="client='http'"):
        make_client(FakeSpark(BASE), "polaris", "jvm")


def test_unknown_client_kind():
    with pytest.raises(TransactionUsageError, match="client must be"):
        make_client(FakeSpark(BASE), "polaris", "grpc")


class _JavaClass:
    def __init__(self, name):
        self._name = name

    def getSimpleName(self):  # noqa: N802
        return self._name


class _JavaExc:
    def __init__(self, name, msg):
        self._cls, self._msg = _JavaClass(name), msg

    def getClass(self):  # noqa: N802
        return self._cls

    def getMessage(self):  # noqa: N802
        return self._msg


class _Py4JError(Exception):
    def __init__(self, name, msg):
        super().__init__(msg)
        self.java_exception = _JavaExc(name, msg)


@pytest.mark.parametrize("java_name,status", [
    ("CommitFailedException", 409),
    ("NoSuchTableException", 404),
    ("ForbiddenException", 403),
    ("ServiceUnavailableException", 503),
    ("CommitStateUnknownException", 500),
    ("SomethingNew", 500),  # unknown -> treated as "outcome unknown", so the commit gets verified
])
def test_java_exceptions_map_to_statuses(java_name, status):
    err = _to_catalog_error(_Py4JError(java_name, "boom"))
    assert isinstance(err, CatalogError) and err.status == status and err.error_type == java_name


def test_conflict_response_for_an_applied_commit_counts_as_committed():
    cat = FakeCatalog()
    cat.create("db.a")
    ops = FakeOps(cat)
    with atomic(None, CATALOG, ["db.a"], client=cat, ops=ops) as tx:
        head = ops.write("db.a")
        # applied, then the client's internal retry reports a requirement failure
        cat.failures = [("after", CatalogError(409, "CommitFailedException", "branch main has changed"))]
    assert tx.state is State.COMMITTED and cat.main("db.a") == head
    assert [b for b in cat.refs("db.a") if b != "main"] == []
    assert ref("db.a") in ops.refreshed
