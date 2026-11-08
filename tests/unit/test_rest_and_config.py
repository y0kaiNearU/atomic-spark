from __future__ import annotations

import json

import pytest

from atomic_spark.config import CatalogConfig
from atomic_spark.errors import CatalogError, TransactionUsageError
from atomic_spark.model import TableRef
from atomic_spark.rest import RestCatalogClient


class Resp:
    def __init__(self, status, body=None):
        self.status_code = status
        self._body = body
        self.text = json.dumps(body) if body is not None else ""

    def json(self):
        if self._body is None:
            raise ValueError("no body")
        return self._body


class FakeHttp:
    def __init__(self):
        self.headers = {}
        self.calls = []
        self.tokens = 0

    def post(self, url, data=None, timeout=None):
        self.calls.append(("TOKEN", url, data))
        self.tokens += 1
        return Resp(200, {"access_token": f"tok{self.tokens}", "expires_in": 3600})

    def request(self, method, url, headers=None, timeout=None, **kw):
        self.calls.append((method, url, headers, kw))
        if url.endswith("/v1/config"):
            return Resp(200, {"overrides": {"prefix": "atomic"}, "defaults": {}})
        if url.endswith("/transactions/commit"):
            if kw["json"]["table-changes"][0]["identifier"]["name"] == "conflict":
                return Resp(409, {"error": {"type": "CommitFailedException", "message": "nope", "code": 409}})
            return Resp(204)
        return Resp(200, {"metadata": {"table-uuid": "u"}})


def client(http, **kw):
    return RestCatalogClient("http://polaris/api/catalog/", warehouse="atomic", credential="root:s3cr3t",
                             scope="PRINCIPAL_ROLE:ALL", session=http, **kw)


def test_token_prefix_and_namespace_encoding():
    http = FakeHttp()
    c = client(http)
    c.load_table(TableRef(("db", "sub"), "t"))
    c.load_table(TableRef(("db",), "t2"))
    token_call = http.calls[0]
    assert token_call[2] == {"grant_type": "client_credentials", "client_secret": "s3cr3t",
                             "scope": "PRINCIPAL_ROLE:ALL", "client_id": "root"}
    urls = [c[1] for c in http.calls if c[0] == "GET"]
    assert urls[0] == "http://polaris/api/catalog/v1/config"
    assert urls[1] == "http://polaris/api/catalog/v1/atomic/namespaces/db%1Fsub/tables/t"
    assert http.tokens == 1  # cached
    assert http.calls[1][2]["Authorization"] == "Bearer tok1"


def test_secret_only_credential_and_static_token():
    http = FakeHttp()
    c = RestCatalogClient("http://p", credential="justsecret", session=http)
    c._fetch_token()
    assert "client_id" not in http.calls[0][2] and http.calls[0][2]["client_secret"] == "justsecret"
    http2 = FakeHttp()
    c2 = RestCatalogClient("http://p", token="static", session=http2)
    c2.load_table(TableRef(("db",), "t"))
    assert http2.tokens == 0 and http2.calls[-1][2]["Authorization"] == "Bearer static"


def test_commit_errors_are_typed():
    c = client(FakeHttp())
    c.commit_transaction([{"identifier": {"namespace": ["db"], "name": "ok"}, "requirements": [], "updates": []}])
    with pytest.raises(CatalogError) as ei:
        c.commit_transaction([{"identifier": {"namespace": ["db"], "name": "conflict"}, "requirements": [], "updates": []}])
    assert ei.value.status == 409 and ei.value.error_type == "CommitFailedException"


class FakeConf:
    def __init__(self, d):
        self.d = d

    def getAll(self):  # noqa: N802 - mirrors PySpark
        return dict(self.d)


class FakeSpark:
    def __init__(self, d):
        self.conf = FakeConf(d)

    @property
    def sparkContext(self):  # noqa: N802
        raise RuntimeError("no context (Spark Connect)")


BASE = {
    "spark.sql.catalog.polaris": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.polaris.type": "rest",
    "spark.sql.catalog.polaris.uri": "http://polaris:8181/api/catalog/",
    "spark.sql.catalog.polaris.warehouse": "atomic",
    "spark.sql.catalog.polaris.credential": "root:s3cr3t",
    "spark.sql.catalog.polaris.scope": "PRINCIPAL_ROLE:ALL",
    "spark.sql.catalog.polaris.header.Polaris-Realm": "POLARIS",
}


def test_config_from_spark():
    cfg = CatalogConfig.from_spark(FakeSpark(BASE), "polaris")
    assert cfg.uri == "http://polaris:8181/api/catalog"
    assert (cfg.warehouse, cfg.credential, cfg.scope) == ("atomic", "root:s3cr3t", "PRINCIPAL_ROLE:ALL")
    assert cfg.headers == {"Polaris-Realm": "POLARIS"}


@pytest.mark.parametrize("override,match", [
    ({"spark.sql.catalog.polaris": None}, "not configured"),
    ({"spark.sql.catalog.polaris.type": "hive"}, "not an Iceberg REST"),
    ({"spark.sql.catalog.polaris.uri": None}, "uri"),
    ({"spark.sql.catalog.polaris.credential": None}, "credential"),
])
def test_config_errors(override, match):
    d = {k: v for k, v in {**BASE, **override}.items() if v is not None}
    with pytest.raises(TransactionUsageError, match=match):
        CatalogConfig.from_spark(FakeSpark(d), "polaris")
