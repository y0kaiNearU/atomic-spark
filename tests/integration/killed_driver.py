"""Driver for test_killed_driver: opens an atomic block, writes, reports the branch, then waits to be killed.

    python killed_driver.py <catalog> <table>      (Spark conf as JSON in ATOMIC_IT_SPARK_CONF)
"""

import json
import os
import sys
import time

from pyspark.sql import SparkSession

from atomic_spark import atomic

catalog, table = sys.argv[1], sys.argv[2]
b = SparkSession.builder.master("local[1]").appName("atomic-spark-killed-driver")
for k, v in json.loads(os.environ["ATOMIC_IT_SPARK_CONF"]).items():
    b = b.config(k, v)
spark = b.getOrCreate()

tx = atomic(spark, catalog, tables=[table]).__enter__()
spark.sql(f"INSERT INTO {catalog}.{table} VALUES (99, 'orphan')")
print(f"WRITTEN {tx.branch}", flush=True)
time.sleep(600)
