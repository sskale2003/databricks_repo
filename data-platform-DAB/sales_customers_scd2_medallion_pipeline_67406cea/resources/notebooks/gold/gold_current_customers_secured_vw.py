from pyspark import pipelines as dp
from pyspark.sql import functions as F

from resources.notebooks.config.pipeline_config import (
    GOLD_SCHEMA,
    TARGET_CATALOG,
)


@dp.view(
    name="current_customers_secured",
    comment="Authorized view for current customers with security controls (column masking and row filtering) applied via security pipeline."
)
def gold_current_customers_secured():
    """
    View on top of current_customers table.
    This view will have column masking and row filtering applied
    by the data security pipeline.
    
    Returns:
        DataFrame: All columns from current_customers table
    """
    return (
        spark.read.table(f"{TARGET_CATALOG}.{GOLD_SCHEMA}.current_customers")
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
            "record_effective_from",
            "record_effective_to",
            "environment_name",
            "source_table_name",
        )
    )