"""
Gold layer — Daily aggregated NYTaxi trip statistics.

Materialized View : config.nytaxi_daily_summary  (<catalog>.<gold_schema>.nytaxi_daily_summary)
Source            : config.nytaxi_cleansed

All parameters are centralised in config.py at the pipeline root.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
import config


@dp.materialized_view(
    name=config.nytaxi_daily_summary,
    comment=(
        "Gold — Daily aggregated NY Taxi trip statistics. "
        "One row per calendar day: trip count, average fare amount, "
        "average trip distance, and average passenger count. "
        "Liquid-clustered on trip_date for efficient date-range scans."
    ),
    cluster_by=["trip_date"],
    table_properties={
        "quality": "gold",
    },
)
def nytaxi_daily_summary():
    """
    Gold aggregation: reads silver batch, aggregates to one row per calendar day.
    Source and target table names come from config.py.
    """
    return (
        spark.read.table(config.nytaxi_cleansed)
        .withColumn("trip_date", F.to_date("tpep_pickup_datetime"))
        .groupBy("trip_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_amount"),
            F.round(F.avg("trip_distance"), 3).alias("avg_trip_distance"),
            F.round(F.avg("passenger_count"), 2).alias("avg_passenger_count"),
        )
    )
