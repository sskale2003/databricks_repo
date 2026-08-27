"""
Pipeline configuration — single source of truth for all pipeline parameters.

Import this module in every transformation file:

    import config

All parameters are read from Spark conf. Override any value for a specific
environment in Pipeline Settings → Configuration.

Parameters
----------
catalog              Target Unity Catalog            (default: dev)
bronze_schema        Bronze layer schema             (default: bronze)
silver_schema        Silver layer schema             (default: silver)
gold_schema          Gold layer schema               (default: gold)
source_path          Source CSV directory            (default: /databricks-datasets/nyctaxi/tripdata/yellow/)
path_glob_filter     Glob pattern for file ingestion (default: yellow_tripdata_2019-*.csv.gz)
log_retention_days   Delta log retention in days     (default: 30)
"""

from pyspark.sql import SparkSession

_spark = SparkSession.getActiveSession()

# ── Core parameters ────────────────────────────────────────────────────────────
catalog            = _spark.conf.get("catalog",            "dev")
bronze_schema      = _spark.conf.get("bronze_schema",      "bronze")
silver_schema      = _spark.conf.get("silver_schema",      "silver")
gold_schema        = _spark.conf.get("gold_schema",        "gold")
source_path        = _spark.conf.get("source_path",        "/databricks-datasets/nyctaxi/tripdata/yellow/")
path_glob_filter   = _spark.conf.get("path_glob_filter",   "yellow_tripdata_2019-*.csv.gz")
log_retention_days = _spark.conf.get("log_retention_days", "30")

# ── Derived fully-qualified table names ────────────────────────────────────────
nytaxi_raw           = f"{catalog}.{bronze_schema}.nytaxi_raw"
nytaxi_cleansed      = f"{catalog}.{silver_schema}.nytaxi_cleansed"
nytaxi_daily_summary = f"{catalog}.{gold_schema}.nytaxi_daily_summary"
