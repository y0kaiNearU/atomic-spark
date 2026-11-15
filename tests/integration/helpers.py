"""Small helpers shared by the integration tests."""

from __future__ import annotations

from atomic_spark.model import TableRef

CAT = "polaris"


def rows(spark, t, where="true"):
    return spark.sql(f"SELECT count(*) FROM {CAT}.{t} WHERE {where}").collect()[0][0]


def refs(rest, t):
    md = rest.load_table(TableRef.parse(t, CAT))
    return sorted((md.get("refs") or {}).keys())


def main_id(rest, t):
    md = rest.load_table(TableRef.parse(t, CAT))
    return ((md.get("refs") or {}).get("main") or {}).get("snapshot-id")
