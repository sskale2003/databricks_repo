from pyspark import pipelines as dp
from pyspark.sql import functions as F

from resources.notebooks.config.pipeline_config import (
    BRONZE_SCHEMA,
    ENVIRONMENT,
    SOURCE_TABLE,
    TARGET_CATALOG,
    dataset_options,
)


@dp.materialized_view(
    **dataset_options(
        name=f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.sales_customers",
        comment="Bronze customer snapshot ingested from the configured Unity Catalog source table.",
        quality="bronze",
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