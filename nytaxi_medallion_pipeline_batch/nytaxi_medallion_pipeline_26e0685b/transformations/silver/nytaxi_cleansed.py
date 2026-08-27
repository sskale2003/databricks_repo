"""
Silver layer — Cleansed and conformed NYTaxi data.

Streaming table : config.nytaxi_cleansed  (<catalog>.<silver_schema>.nytaxi_cleansed)
Source          : config.nytaxi_raw

All parameters are centralised in config.py at the pipeline root.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
import config


@dp.table(
    name=config.nytaxi_cleansed,
    comment=(
        "Silver — Cleansed and type-safe NY Taxi yellow trip records. "
        "Rows failing quality expectations are dropped. "
        "Columns are normalised to snake_case; "
        "_processed_at records the transform timestamp."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_or_drop("valid_pickup_datetime",      "tpep_pickup_datetime IS NOT NULL")
@dp.expect_or_drop("valid_dropoff_datetime",     "tpep_dropoff_datetime IS NOT NULL")
@dp.expect_or_drop("positive_fare_amount",       "fare_amount > 0")
@dp.expect_or_drop("positive_passenger_count",   "passenger_count > 0")
@dp.expect_or_drop("non_negative_trip_distance", "trip_distance >= 0")
@dp.expect_or_drop("valid_vendor_id",            "vendor_id IS NOT NULL")
def nytaxi_cleansed():
    """
    Silver transformation: reads from bronze, casts/renames columns, applies DQ expectations.
    Source and target table names come from config.py.
    """
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table(config.nytaxi_raw)
        .select(
            F.col("VendorID").cast("integer").alias("vendor_id"),
            F.col("RatecodeID").cast("integer").alias("rate_code_id"),
            F.col("store_and_fwd_flag"),
            F.col("PULocationID").cast("integer").alias("pu_location_id"),
            F.col("DOLocationID").cast("integer").alias("do_location_id"),
            F.col("payment_type").cast("integer"),
            F.col("tpep_pickup_datetime").cast("timestamp"),
            F.col("tpep_dropoff_datetime").cast("timestamp"),
            F.col("passenger_count").cast("integer"),
            F.col("trip_distance").cast("double"),
            F.col("fare_amount").cast("double"),
            F.col("extra").cast("double"),
            F.col("mta_tax").cast("double"),
            F.col("tip_amount").cast("double"),
            F.col("tolls_amount").cast("double"),
            F.col("improvement_surcharge").cast("double"),
            F.col("total_amount").cast("double"),
            F.col("congestion_surcharge").cast("double"),
            F.col("_ingested_at"),
        )
        .withColumn("_processed_at", F.current_timestamp())
    )
