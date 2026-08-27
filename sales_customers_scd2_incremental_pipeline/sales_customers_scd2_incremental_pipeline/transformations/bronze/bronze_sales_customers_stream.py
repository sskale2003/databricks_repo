from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.config.pipeline_config import ENVIRONMENT, SOURCE_TABLE, dataset_options


@dp.streaming_table(
    **dataset_options(
        "bronze_sales_customers_stream",
        comment="Bronze streaming table — ingests only new customer records appended to the source since the last run.",
        cluster_by=["customerID"],
    )
)
def bronze_sales_customers_stream():
    """
    Incremental bronze layer.
    Uses spark.readStream.table() so only rows appended to SOURCE_TABLE since the
    last pipeline run are processed — no full scan on every trigger.

    Production note: if the source table has Change Data Feed enabled, switch to:
        spark.readStream.option("readChangeFeed", "true").table(SOURCE_TABLE)
    to also capture UPDATE and DELETE events from the source.
    """
    return (
        spark.readStream.table(SOURCE_TABLE)
        .select(
            "customerID",
            "first_name",
            "last_name",
            "email_address",
            "phone_number",
            "address",
            "city",
            "state",
            "country",
            "continent",
            "postal_zip_code",
            "gender",
        )
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("environment_name", F.lit(ENVIRONMENT))
        .withColumn("source_table_name", F.lit(SOURCE_TABLE))
    )
