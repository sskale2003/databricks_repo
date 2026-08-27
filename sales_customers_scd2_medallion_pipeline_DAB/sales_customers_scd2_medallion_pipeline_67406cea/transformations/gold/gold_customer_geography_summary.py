from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.config.pipeline_config import DATASET_NAMES, dataset_options


@dp.materialized_view(
    **dataset_options(
        "gold_customer_geography_summary",
        comment="Gold geography summary over the current-state customer dimension.",
        cluster_by=["continent", "country"],
    )
)
def gold_customer_geography_summary():
    return (
        spark.read.table(DATASET_NAMES["gold_current_customers"])
        .groupBy("continent", "country", "state")
        .agg(
            F.count("*").alias("customer_count"),
            F.countDistinct("city").alias("distinct_city_count"),
            F.max("record_effective_from").alias("latest_dimension_change_ts"),
        )
    )
