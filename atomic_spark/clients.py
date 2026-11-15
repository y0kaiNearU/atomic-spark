"""Chooses how atomic-spark talks to the catalog.

"jvm"   Iceberg's own Java REST client inside Spark's JVM (jvm.py). Authentication is whatever Spark's
        catalog config does (Keycloak, Polaris OAuth, rest.auth.type auth managers, ...). Needs a classic
        SparkSession; not available with Spark Connect.
"http"  The Python client in rest.py. Supports OAuth2 client credentials or a static token read from
        the Spark catalog config. Works with Spark Connect.
"auto"  (default) "jvm" when available, otherwise "http". The ATOMIC_SPARK_CLIENT environment variable
        overrides "auto".
"""

from __future__ import annotations

import os

from .config import CatalogConfig
from .errors import TransactionUsageError
from .jvm import jvm_available, jvm_client
from .rest import RestCatalogClient

KINDS = ("auto", "jvm", "http")


def make_client(spark, catalog: str, kind: str = "auto"):
    if kind == "auto":
        kind = os.environ.get("ATOMIC_SPARK_CLIENT", "auto").strip().lower() or "auto"
    if kind not in KINDS:
        raise TransactionUsageError(f"client must be one of {KINDS} or a client object, not {kind!r}")
    if kind == "auto":
        kind = "jvm" if jvm_available(spark) else "http"
    if kind == "jvm":
        if not jvm_available(spark):
            raise TransactionUsageError(
                "client='jvm' needs a classic SparkSession with the Iceberg Spark runtime on the classpath "
                "(Spark Connect has no JVM access); use client='http' instead"
            )
        return jvm_client(spark, catalog)
    return RestCatalogClient.from_config(CatalogConfig.from_spark(spark, catalog))
