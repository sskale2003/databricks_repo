from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.config.pipeline_config import ENVIRONMENT, SOURCE_TABLE, dataset_options


@dp.materialized_view(
    **dataset_options(
        "bronze_sales_customers",
        comment="Bronze customer snapshot ingested from the configured Unity Catalog source table.",
        cluster_by=["customerID"],
    )
)
def bronze_sales_customers():
    return (
        spark.read.table(SOURCE_TABLE)
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
