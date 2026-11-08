"""The few Spark-session operations a transaction needs, behind one seam so they can be faked in tests."""

from __future__ import annotations

from .model import TableRef

WAP_BRANCH = "spark.wap.branch"
WAP_ID = "spark.wap.id"


class SparkOps:
    def __init__(self, spark, catalog: str):
        self.spark = spark
        self.catalog = catalog

    def current_wap(self) -> str | None:
        return self.spark.conf.get(WAP_BRANCH, None) or self.spark.conf.get(WAP_ID, None)

    def set_wap(self, branch: str) -> None:
        self.spark.conf.set(WAP_BRANCH, branch)

    def clear_wap(self) -> None:
        self.spark.conf.unset(WAP_BRANCH)

    def refresh(self, ref: TableRef) -> None:
        self.spark.sql(f"REFRESH TABLE {ref.spark_name(self.catalog)}")

    def enable_wap(self, ref: TableRef) -> None:
        self.spark.sql(f"ALTER TABLE {ref.spark_name(self.catalog)} SET TBLPROPERTIES ('write.wap.enabled'='true')")
