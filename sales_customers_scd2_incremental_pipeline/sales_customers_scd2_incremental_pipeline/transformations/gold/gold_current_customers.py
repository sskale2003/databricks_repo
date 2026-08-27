from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.config.pipeline_config import DATASET_NAMES, dataset_options


@dp.materialized_view(
    **dataset_options(
        "gold_current_customers",
        comment="Gold current-state customer dimension derived from the incremental SCD Type 2 history table.",
        cluster_by=["country", "state"],
    )
)
def gold_current_customers():
    """Current active customer records — rows where __END_AT is NULL are the latest version."""
    return (
        spark.read.table(DATASET_NAMES["silver_customers_scd2_incremental"])
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
