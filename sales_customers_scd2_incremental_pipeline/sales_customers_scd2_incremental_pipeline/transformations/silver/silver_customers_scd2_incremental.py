from pyspark import pipelines as dp

from transformations.config.pipeline_config import (
    DATASET_NAMES,
    SEQUENCE_COLUMN,
    TRACK_HISTORY_EXCEPT_COLUMNS,
    dataset_options,
)


dp.create_streaming_table(
    **dataset_options(
        "silver_customers_scd2_incremental",
        comment="Silver SCD Type 2 customer dimension — built incrementally via event-by-event CDC from the normalized streaming layer.",
        cluster_by=["customer_id"],
    )
)


# create_auto_cdc_flow (event-by-event CDC)
# ------------------------------------------
# Key difference from the full-refresh pipeline:
#   Full refresh uses create_auto_cdc_from_snapshot_flow which diffs full snapshots.
#   This incremental pipeline uses create_auto_cdc_flow which processes individual
#   change events ordered by SEQUENCE_COLUMN — only new events are applied each run.
#
# sequence_by (SEQUENCE_COLUMN): must be a monotonically increasing column so the
#   engine can order events correctly per customer_id. Defaults to ingestion_ts.
#   For production workloads backed by a source with CDF, consider using the
#   CDF commit_version column as the sequence to guarantee total ordering.
#
# track_history_except_column_list: ingestion_ts and record_hash change on every
#   batch and must be excluded — otherwise every new event would open a new SCD2
#   version even when no business attributes changed.
dp.create_auto_cdc_flow(
    target=DATASET_NAMES["silver_customers_scd2_incremental"],
    source=DATASET_NAMES["silver_sales_customers_normalized"],
    keys=["customer_id"],
    sequence_by=SEQUENCE_COLUMN,
    stored_as_scd_type=2,
    track_history_except_column_list=TRACK_HISTORY_EXCEPT_COLUMNS,
)
