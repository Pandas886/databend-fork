#!/usr/bin/env python3
"""Prepare local filesystem Paimon tables for stateful regression.

Run: uv run --project tests/sqllogictests/scripts prepare_paimon_fs_data.py
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

warehouse = os.environ.get(
    "PAIMON_WAREHOUSE",
    str(Path(__file__).resolve().parents[2] / "data" / "paimon_warehouse"),
)
Path(warehouse).mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder.appName("prepare-paimon-fs-data")
    .master("local[4]")
    .config(
        "spark.jars.packages",
        "org.apache.paimon:paimon-spark-3.5_2.12:1.4.1",
    )
    .config(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
    )
    .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
    .config("spark.sql.catalog.paimon.warehouse", f"file://{warehouse}")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS paimon.regression")

spark.sql("DROP TABLE IF EXISTS paimon.regression.append_t")
spark.sql(
    """
CREATE TABLE paimon.regression.append_t (
  part INT,
  id INT,
  name STRING
) USING paimon
PARTITIONED BY (part)
"""
)

for part, name in [(0, "a0"), (1, "a1"), (2, "b0"), (3, "b1")]:
    spark.sql(
        f"""
INSERT INTO paimon.regression.append_t PARTITION (part = {part})
SELECT {part}, '{name}'
"""
    )

spark.sql("DROP TABLE IF EXISTS paimon.regression.pk_t")
spark.sql(
    """
CREATE TABLE paimon.regression.pk_t (
  id INT,
  name STRING
) USING paimon
TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')
"""
)
spark.sql("INSERT INTO paimon.regression.pk_t VALUES (1, 'old')")
spark.sql("INSERT INTO paimon.regression.pk_t VALUES (1, 'new')")

print("Prepared Paimon filesystem warehouse at", warehouse)
