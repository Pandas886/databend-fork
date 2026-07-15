#!/usr/bin/env python3
"""Prepare filesystem or S3 Paimon tables for stateful regression.

Run: uv run --project tests/sqllogictests/scripts prepare_paimon_fs_data.py

Verify Databend writes (after sqllogictest):
  PAIMON_VERIFY_ONLY=1 uv run --project tests/sqllogictests/scripts \\
    tests/sqllogictests/scripts/prepare_paimon_fs_data.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

warehouse = os.environ.get(
    "PAIMON_WAREHOUSE",
    str(Path(__file__).resolve().parents[2] / "data" / "paimon_warehouse"),
)
if "://" in warehouse:
    warehouse_uri = warehouse
else:
    Path(warehouse).mkdir(parents=True, exist_ok=True)
    warehouse_uri = f"file://{warehouse}"

packages = "org.apache.paimon:paimon-spark-3.5_2.12:1.4.1"
if warehouse.startswith("s3://"):
    packages += ",org.apache.paimon:paimon-s3:1.4.1"

builder = (
    SparkSession.builder.appName("prepare-paimon-fs-data")
    .master("local[4]")
    .config("spark.jars.packages", packages)
    .config(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
    )
    .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
    .config("spark.sql.catalog.paimon.warehouse", warehouse_uri)
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
)

if warehouse.startswith("s3://"):
    builder = (
        builder.config(
            "spark.sql.catalog.paimon.s3.endpoint",
            os.environ["PAIMON_S3_ENDPOINT"],
        )
        .config(
            "spark.sql.catalog.paimon.s3.access-key",
            os.environ["PAIMON_S3_ACCESS_KEY"],
        )
        .config(
            "spark.sql.catalog.paimon.s3.secret-key",
            os.environ["PAIMON_S3_SECRET_KEY"],
        )
        .config("spark.sql.catalog.paimon.s3.path.style.access", "true")
        .config("spark.sql.catalog.paimon.s3.region", "us-east-1")
    )

spark = builder.getOrCreate()


def _spark_table_rows(table: str):
    return spark.sql(f"SELECT * FROM paimon.regression.{table}").collect()


def _assert_table_rows(table: str, expect_rows: int) -> None:
    """Require the official Paimon reader to read every Databend-written table."""
    rows = _spark_table_rows(table)
    assert len(rows) == expect_rows, f"{table} expected {expect_rows} rows, got {len(rows)}"


def verify_databend_writes() -> None:
    """Assert row counts and primary-key finals after Databend e2e writes."""
    _assert_table_rows("write_append", 10)

    append_part_count = spark.sql(
        "SELECT count(*) AS c FROM paimon.regression.write_append_part"
    ).collect()[0]["c"]
    assert append_part_count == 4, (
        f"write_append_part expected 4 rows, got {append_part_count}"
    )

    pk_rows = {
        (r["id"], r["value"])
        for r in spark.sql(
            "SELECT id, value FROM paimon.regression.write_pk"
        ).collect()
    }
    assert pk_rows == {(1, "new"), (2, "x")}, f"write_pk unexpected rows: {pk_rows}"

    pk_part_stats = spark.sql(
        """
SELECT count(*) AS rows, sum(id) AS id_sum,
       sum(CASE WHEN value = 'new' THEN 1 ELSE 0 END) AS updated
FROM paimon.regression.write_pk_part
"""
    ).collect()[0]
    assert tuple(pk_part_stats) == (10000, 49995000, 1), (
        f"write_pk_part unexpected stats: {tuple(pk_part_stats)}"
    )

    print("Verified Databend Paimon writes")


def prepare_tables() -> None:
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

    # Empty write targets for Databend e2e (DROP then CREATE for idempotency).
    spark.sql("DROP TABLE IF EXISTS paimon.regression.write_append")
    spark.sql(
        """
CREATE TABLE paimon.regression.write_append (id INT, value STRING)
USING paimon TBLPROPERTIES ('bucket'='-1')
"""
    )

    spark.sql("DROP TABLE IF EXISTS paimon.regression.write_append_part")
    spark.sql(
        """
CREATE TABLE paimon.regression.write_append_part (id INT, value STRING, part INT)
USING paimon PARTITIONED BY (part) TBLPROPERTIES ('bucket'='-1')
"""
    )

    spark.sql("DROP TABLE IF EXISTS paimon.regression.write_pk")
    spark.sql(
        """
CREATE TABLE paimon.regression.write_pk (id INT, value STRING)
USING paimon TBLPROPERTIES ('primary-key'='id', 'bucket'='4')
"""
    )

    spark.sql("DROP TABLE IF EXISTS paimon.regression.write_pk_part")
    spark.sql(
        """
CREATE TABLE paimon.regression.write_pk_part (id INT, value STRING, part INT)
USING paimon PARTITIONED BY (part)
TBLPROPERTIES ('primary-key'='part,id', 'bucket'='4')
"""
    )

    # Cluster write regression: bucket counts below / near / above typical writer lanes.
    for buckets in (2, 8, 64):
        table = f"write_pk_part_b{buckets}"
        spark.sql(f"DROP TABLE IF EXISTS paimon.regression.{table}")
        spark.sql(
            f"""
CREATE TABLE paimon.regression.{table} (id INT, value STRING, part INT)
USING paimon PARTITIONED BY (part)
TBLPROPERTIES ('primary-key'='part,id', 'bucket'='{buckets}')
"""
        )

    print("Prepared Paimon warehouse at", warehouse)


if __name__ == "__main__":
    try:
        if os.environ.get("PAIMON_VERIFY_ONLY", "") == "1":
            verify_databend_writes()
        else:
            prepare_tables()
    finally:
        spark.stop()
    sys.exit(0)
