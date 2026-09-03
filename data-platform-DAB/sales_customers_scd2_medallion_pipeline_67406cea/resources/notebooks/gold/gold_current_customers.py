from pyspark import pipelines as dp
from pyspark.sql import functions as F

from resources.notebooks.config.pipeline_config import (
    GOLD_SCHEMA,
    SILVER_SCHEMA,
    TARGET_CATALOG,
    dataset_options,
)


@dp.materialized_view(
    **dataset_options(
        name=f"{TARGET_CATALOG}.{GOLD_SCHEMA}.current_customers",
        comment="Gold current-state customer dimension derived from the SCD Type 2 history table.",
        quality="gold",
        cluster_by=["country", "state"],
    )
)
def gold_current_customers():
    return (
        spark.read.table(f"{TARGET_CATALOG}.{SILVER_SCHEMA}.customers_scd2")
        .filter(F.col("__END_AT").isNull())
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "full_name",
            "email_address",
            "phone_number",
            "address",
            "city",
            "state",
            "country",
            "continent",
            "postal_zip_code",
            "gender",
            F.col("__START_AT").alias("record_effective_from"),
            F.col("__END_AT").alias("record_effective_to"),
            "environment_name",
            "source_table_name",
        )
    )