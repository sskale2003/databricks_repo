from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.config.pipeline_config import (
    DATASET_NAMES,
    ENVIRONMENT,
    SOURCE_TABLE,
    dataset_options,
)


# Business columns used in the record-level hash — drives SCD2 change detection.
# Add or remove columns here to control what counts as a meaningful customer change.
BUSINESS_COLUMNS = [
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
]


@dp.streaming_table(
    **dataset_options(
        "silver_sales_customers_normalized",
        comment="Silver normalized streaming layer — standardizes and hashes each incremental bronze record for downstream SCD2 processing.",
        cluster_by=["country", "state"],
    )
)
def silver_sales_customers_normalized():
    """
    Incremental silver normalization layer.
    Reads the bronze streaming table via readStream so only new bronze records
    are processed each trigger.  No dropDuplicates at this stage — deduplication
    by customer_id is handled by the SCD2 APPLY CHANGES layer downstream.
    """
    standardized_df = (
        spark.readStream.table(DATASET_NAMES["bronze_sales_customers_stream"])
        .filter(F.col("customerID").isNotNull())
        .select(
            F.col("customerID").cast("bigint").alias("customer_id"),
            F.trim(F.col("first_name")).alias("first_name"),
            F.trim(F.col("last_name")).alias("last_name"),
            F.lower(F.trim(F.col("email_address"))).alias("email_address"),
            F.trim(F.col("phone_number")).alias("phone_number"),
            F.trim(F.col("address")).alias("address"),
            F.initcap(F.trim(F.col("city"))).alias("city"),
            F.upper(F.trim(F.col("state"))).alias("state"),
            F.trim(F.col("country")).alias("country"),
            F.trim(F.col("continent")).alias("continent"),
            F.col("postal_zip_code").cast("string").alias("postal_zip_code"),
            F.lower(F.trim(F.col("gender"))).alias("gender"),
            F.col("ingestion_ts"),
            F.col("environment_name"),
            F.col("source_table_name"),
        )
    )

    with_name = standardized_df.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")),
    )

    return with_name.withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(column_name).cast("string"), F.lit(""))
                    for column_name in BUSINESS_COLUMNS
                ],
            ),
            256,
        ),
    )
