"""Reads the Iceberg REST catalog settings from the Spark session's configuration."""

from __future__ import annotations

from dataclasses import dataclass, field

from .errors import TransactionUsageError


@dataclass
class CatalogConfig:
    name: str
    uri: str
    warehouse: str | None = None
    credential: str | None = None
    token: str | None = None
    scope: str = "catalog"
    oauth2_server_uri: str | None = None
    headers: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_spark(cls, spark, catalog: str) -> "CatalogConfig":
        prefix = f"spark.sql.catalog.{catalog}"
        conf = _all_conf(spark)
        if prefix not in conf:
            raise TransactionUsageError(f"Spark catalog {catalog!r} is not configured ({prefix} is not set)")
        opts = {k[len(prefix) + 1:]: v for k, v in conf.items() if k.startswith(prefix + ".")}
        is_rest = opts.get("type") == "rest" or opts.get("catalog-impl", "").endswith("RESTCatalog")
        if not is_rest:
            raise TransactionUsageError(
                f"Spark catalog {catalog!r} is not an Iceberg REST catalog; atomic-spark needs one "
                "that supports multi-table commits (e.g. Apache Polaris)"
            )
        if not opts.get("uri"):
            raise TransactionUsageError(f"{prefix}.uri is not set")
        if not (opts.get("credential") or opts.get("token")):
            raise TransactionUsageError(f"Neither {prefix}.credential nor {prefix}.token is set")
        return cls(
            name=catalog,
            uri=opts["uri"].rstrip("/"),
            warehouse=opts.get("warehouse"),
            credential=opts.get("credential"),
            token=opts.get("token"),
            scope=opts.get("scope", "catalog"),
            oauth2_server_uri=opts.get("oauth2-server-uri"),
            headers={k[len("header."):]: v for k, v in opts.items() if k.startswith("header.")},
        )


def _all_conf(spark) -> dict[str, str]:
    conf: dict[str, str] = {}
    try:
        conf.update(dict(spark.sparkContext.getConf().getAll()))
    except Exception:  # noqa: BLE001 - e.g. Spark Connect has no SparkContext
        pass
    get_all = getattr(spark.conf, "getAll", None)
    if get_all is not None:
        conf.update(get_all() if callable(get_all) else get_all)
    return conf
