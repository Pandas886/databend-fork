#!/usr/bin/env python3
"""Prepare Paimon append/PK tables for stateful regression.

Run with: uv run tests/sqllogictests/scripts/prepare_paimon_test_data.py
Requires Spark + Paimon Spark connector (see docker-compose-paimon.yml).
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

warehouse = os.environ.get("PAIMON_WAREHOUSE", "s3://paimon-warehouse/")
s3_endpoint = os.environ.get("PAIMON_S3_ENDPOINT", "http://127.0.0.1:9010")
access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")
region = os.environ.get("AWS_REGION", "us-east-1")

spark = (
    SparkSession.builder.appName("prepare-paimon-test-data")
    .config(
        "spark.jars.packages",
        "org.apache.paimon:paimon-spark-3.5_2.12:1.4.1,org.apache.hadoop:hadoop-aws:3.3.4",
    )
    .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
    .config("spark.sql.catalog.paimon.warehouse", warehouse)
    .config("spark.sql.catalog.paimon.s3.endpoint", s3_endpoint)
    .config("spark.sql.catalog.paimon.s3.access-key-id", access_key)
    .config("spark.sql.catalog.paimon.s3.secret-access-key", secret_key)
    .config("spark.sql.catalog.paimon.s3.path-style-access", "true")
    .config("spark.sql.catalog.paimon.client.region", region)
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
spark.sql(
    """
INSERT INTO paimon.regression.append_t PARTITION (part = 0)
SELECT id, CONCAT('a', CAST(id AS STRING)) FROM VALUES (0), (1) AS t(id)
"""
)
spark.sql(
    """
INSERT INTO paimon.regression.append_t PARTITION (part = 1)
SELECT id + 2, CONCAT('b', CAST(id AS STRING)) FROM VALUES (0), (1) AS t(id)
"""
)

spark.sql("DROP TABLE IF EXISTS paimon.regression.pk_t")
spark.sql(
    """
CREATE TABLE paimon.regression.pk_t (
  id INT,
  name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) USING paimon
TBLPROPERTIES ('bucket' = '1')
"""
)
spark.sql("INSERT INTO paimon.regression.pk_t VALUES (1, 'old')")
spark.sql("INSERT INTO paimon.regression.pk_t VALUES (1, 'new')")

print("Prepared Paimon regression tables in", warehouse)
