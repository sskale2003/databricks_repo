from pyspark import pipelines as dp

from resources.notebooks.config.pipeline_config import (
    SILVER_SCHEMA,
    TARGET_CATALOG,
    TRACK_HISTORY_EXCEPT_COLUMNS,
    dataset_options,
)


dp.create_streaming_table(
    **dataset_options(
        name=f"{TARGET_CATALOG}.{SILVER_SCHEMA}.customers_scd2",
        comment="Silver SCD Type 2 customer dimension built from the standardized snapshot.",
        quality="silver",
        cluster_by=["customer_id"],
    )
)


dp.create_auto_cdc_from_snapshot_flow(
    target=f"{TARGET_CATALOG}.{SILVER_SCHEMA}.customers_scd2",
    source=f"{TARGET_CATALOG}.{SILVER_SCHEMA}.sales_customers_snapshot",
    keys=["customer_id"],
    stored_as_scd_type=2,
    track_history_except_column_list=TRACK_HISTORY_EXCEPT_COLUMNS,
)