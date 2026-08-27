from pyspark import pipelines as dp
from pyspark.sql import functions as F

from resources.notebooks.config.pipeline_config import (
    GOLD_SCHEMA,
    TARGET_CATALOG,
    dataset_options,
)


@dp.materialized_view(
    **dataset_options(
        name=f"{TARGET_CATALOG}.{GOLD_SCHEMA}.customer_geography_summary",
        comment="Gold geography summary over the current-state customer dimension.",
        quality="gold",
        cluster_by=["continent", "country"],
    )
)
def gold_customer_geography_summary():
    return (
        spark.read.table(f"{TARGET_CATALOG}.{GOLD_SCHEMA}.current_customers")
        .groupBy("continent", "country", "state")
        .agg(
            F.count("*").alias("customer_count"),
            F.countDistinct("city").alias("distinct_city_count"),
            F.max("record_effective_from").alias("latest_dimension_change_ts"),
        )
    )