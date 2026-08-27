from pyspark import pipelines as dp

from transformations.config.pipeline_config import (
    DATASET_NAMES,
    TRACK_HISTORY_EXCEPT_COLUMNS,
    dataset_options,
)


dp.create_streaming_table(
    **dataset_options(
        "silver_customers_scd2",
        comment="Silver SCD Type 2 customer dimension built from the standardized snapshot.",
        cluster_by=["customer_id"],
    )
)


dp.create_auto_cdc_from_snapshot_flow(
    target=DATASET_NAMES["silver_customers_scd2"],
    source=DATASET_NAMES["silver_sales_customers_snapshot"],
    keys=["customer_id"],
    stored_as_scd_type=2,
    track_history_except_column_list=TRACK_HISTORY_EXCEPT_COLUMNS,
)
