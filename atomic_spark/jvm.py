"""Catalog client that runs inside Spark's JVM, using Iceberg's own Java REST client.

The catalog is built with CatalogUtil from the same spark.sql.catalog.<name>.* options Spark uses,
so authentication is whatever Iceberg itself does for that config: OAuth2 client credentials
against Polaris or Keycloak, a pluggable rest.auth.type auth manager, token refresh, custom headers.
atomic-spark never handles credentials on this path.

Payloads are the same JSON dicts the HTTP client sends; they are parsed into Iceberg's Java
UpdateRequirement / MetadataUpdate objects with Iceberg's own parsers.
"""

from __future__ import annotations

import json
import threading

from .errors import CatalogError, TransactionUsageError
from .model import TableRef

_REST_CATALOG_CLASS = "org.apache.iceberg.rest.RESTCatalog"

# Java exception class (simple name) -> HTTP-like status, so the transaction treats both clients alike.
_STATUS_BY_EXCEPTION = {
    "CommitFailedException": 409,
    "NoSuchTableException": 404,
    "NoSuchNamespaceException": 404,
    "NotAuthorizedException": 401,
    "ForbiddenException": 403,
    "BadRequestException": 400,
    "ValidationException": 400,
    "IllegalArgumentException": 400,
    "UnsupportedOperationException": 406,
    "ServiceUnavailableException": 503,
    "ServiceFailureException": 500,
    "CommitStateUnknownException": 500,
}

_cache: dict[tuple, "JvmCatalogClient"] = {}
_cache_lock = threading.Lock()


def jvm_available(spark) -> bool:
    """True for a classic (non-Connect) session whose classpath has the Iceberg runtime."""
    jvm = getattr(spark, "_jvm", None)
    if jvm is None:
        return False
    try:
        jvm.java.lang.Class.forName(_REST_CATALOG_CLASS)
        return True
    except Exception:  # noqa: BLE001
        return False


def jvm_client(spark, catalog: str) -> "JvmCatalogClient":
    """One client per catalog config and Spark context: building it authenticates, so reuse it."""
    options = _catalog_options(spark, catalog)
    key = (id(spark.sparkContext), catalog, tuple(sorted(options.items())))
    with _cache_lock:
        client = _cache.get(key)
        if client is None:
            client = _cache[key] = JvmCatalogClient(spark, catalog, options)
        return client


class JvmCatalogClient:
    def __init__(self, spark, catalog: str, options: dict[str, str]):
        self._jvm = spark._jvm
        j_options = self._jvm.java.util.HashMap()
        for k, v in options.items():
            j_options.put(k, v)
        hadoop_conf = spark._jsc.hadoopConfiguration()
        try:
            self._catalog = self._jvm.org.apache.iceberg.CatalogUtil.buildIcebergCatalog(catalog, j_options, hadoop_conf)
        except Exception as e:  # noqa: BLE001
            raise _to_catalog_error(e) from e
        cls = self._catalog.getClass().getName()
        if cls != _REST_CATALOG_CLASS:
            raise TransactionUsageError(
                f"Spark catalog {catalog!r} builds a {cls}; atomic-spark needs an Iceberg REST catalog "
                "that supports multi-table commits (e.g. Apache Polaris)"
            )

    def _identifier(self, ref: TableRef):
        return self._jvm.org.apache.iceberg.catalog.TableIdentifierParser.fromJson(json.dumps(ref.rest_identifier()))

    def load_table(self, ref: TableRef) -> dict:
        try:
            table = self._catalog.loadTable(self._identifier(ref))
            metadata = table.operations().current()
            return json.loads(self._jvm.org.apache.iceberg.TableMetadataParser.toJson(metadata))
        except Exception as e:  # noqa: BLE001
            raise _to_catalog_error(e) from e

    def commit_transaction(self, table_changes: list[dict]) -> None:
        payload = json.dumps({"table-changes": table_changes})
        try:
            parser = self._jvm.org.apache.iceberg.rest.requests.CommitTransactionRequestParser
            request = parser.fromJson(payload)
            commits = self._jvm.java.util.ArrayList()
            for change in request.tableChanges():
                # TableCommit.create(...) derives changes from two metadata objects; we already have them.
                commits.add(
                    self._jvm.org.apache.iceberg.catalog.ImmutableTableCommit.builder()
                    .identifier(change.identifier())
                    .requirements(change.requirements())
                    .updates(change.updates())
                    .build()
                )
            self._catalog.commitTransaction(commits)
        except Exception as e:  # noqa: BLE001
            raise _to_catalog_error(e) from e

    def commit_table(self, ref: TableRef, requirements: list[dict], updates: list[dict]) -> None:
        self.commit_transaction([{"identifier": ref.rest_identifier(), "requirements": requirements, "updates": updates}])


def _catalog_options(spark, catalog: str) -> dict[str, str]:
    from .config import _all_conf

    prefix = f"spark.sql.catalog.{catalog}"
    conf = _all_conf(spark)
    if prefix not in conf:
        raise TransactionUsageError(f"Spark catalog {catalog!r} is not configured ({prefix} is not set)")
    return {k[len(prefix) + 1:]: v for k, v in conf.items() if k.startswith(prefix + ".")}


def _to_catalog_error(e: Exception) -> CatalogError:
    """Map a py4j-wrapped Java exception to CatalogError; unknown failures count as 'outcome unknown' (500)."""
    java_exc = getattr(e, "java_exception", None)
    if java_exc is None:
        return CatalogError(500, type(e).__name__, str(e))
    name = java_exc.getClass().getSimpleName()
    message = java_exc.getMessage() or name
    return CatalogError(_STATUS_BY_EXCEPTION.get(name, 500), name, message)
