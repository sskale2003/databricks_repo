"""
Bronze layer — Raw NYTaxi ingestion.

Streaming table : config.nytaxi_raw  (<catalog>.<bronze_schema>.nytaxi_raw)
Source          : config.source_path via Auto Loader

All parameters are centralised in config.py at the pipeline root.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
import config


@dp.table(
    name=config.nytaxi_raw,
    comment=(
        "Bronze — Raw NY Taxi yellow trip records ingested via Auto Loader. "
        "All source columns are retained as-is. "
        "_ingested_at records the micro-batch load timestamp."
    ),
    table_properties={
        "quality": "bronze",
        "delta.logRetentionDuration": f"interval {config.log_retention_days} days",
    },
)
def nytaxi_raw():
    """
    Incrementally ingests NYTaxi yellow-cab CSV files via Auto Loader (cloudFiles).
    All paths and filters are driven by config.py.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaHints",
            "tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP",
        )
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("pathGlobFilter", config.path_glob_filter)
        .load(config.source_path)
        .withColumn("_ingested_at", F.current_timestamp())
    )
