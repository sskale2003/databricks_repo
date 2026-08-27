from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.config.pipeline_config import (
    DATASET_NAMES,
    ENVIRONMENT,
    SOURCE_TABLE,
    dataset_options,
)


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


@dp.materialized_view(
    **dataset_options(
        "silver_sales_customers_snapshot",
        comment="Silver customer snapshot standardized for downstream SCD Type 2 processing.",
        cluster_by=["country", "state"],
    )
)
def silver_sales_customers_snapshot():
    standardized_df = (
        spark.read.table(DATASET_NAMES["bronze_sales_customers"])
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
        )
        .dropDuplicates(["customer_id"])
    )

    hashed_df = standardized_df.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")),
    )

    return (
        hashed_df.withColumn(
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
        .withColumn("snapshot_ts", F.current_timestamp())
        .withColumn("environment_name", F.lit(ENVIRONMENT))
        .withColumn("source_table_name", F.lit(SOURCE_TABLE))
    )
