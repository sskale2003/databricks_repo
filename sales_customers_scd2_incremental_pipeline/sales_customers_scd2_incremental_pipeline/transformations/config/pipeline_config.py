from typing import Dict, List, Optional

from pyspark.sql import SparkSession


CONFIG_KEYS = {
    # Environment-wide config
    "environment": "project.environment",
    "resource_prefix": "project.resource_prefix",
    "source_table": "project.source.table",
    # Generic fallback output config (overridden per-layer when layer keys are set)
    "target_catalog": "project.target.catalog",
    "target_schema": "project.target.schema",
    "target_base_path": "project.target.base_path",
    # Layer-specific output config
    "bronze_catalog": "bronze.target.catalog",
    "bronze_schema": "bronze.target.schema",
    "bronze_volume": "bronze.target.data_volume",
    "bronze_checkpoint_volume": "bronze.target.checkpoint_volume",
    "silver_catalog": "silver.target.catalog",
    "silver_schema": "silver.target.schema",
    "silver_volume": "silver.target.data_volume",
    "silver_checkpoint_volume": "silver.target.checkpoint_volume",
    "gold_catalog": "gold.target.catalog",
    "gold_schema": "gold.target.schema",
    "gold_volume": "gold.target.data_volume",
    "gold_checkpoint_volume": "gold.target.checkpoint_volume",
    # SCD2 & misc
    "sequence_column": "project.scd2.sequence_column",
    "history_exclusions": "project.scd2.track_history_except",
}


# Incremental pipeline dataset registry
# Each key maps to a base table name written to the layer's catalog.schema
DATASET_BASE_NAMES = {
    "bronze_sales_customers_stream": "bronze_sales_customers",
    "silver_sales_customers_normalized": "silver_sales_customers_normalized",
    "silver_customers_scd2_incremental": "silver_customers_scd2",
    "gold_current_customers": "gold_current_customers",
    "gold_customer_geography_summary": "gold_customer_geography_summary",
}


DATASET_LAYERS = {
    "bronze_sales_customers_stream": "bronze",
    "silver_sales_customers_normalized": "silver",
    "silver_customers_scd2_incremental": "silver",
    "gold_current_customers": "gold",
    "gold_customer_geography_summary": "gold",
}


def _spark_session() -> SparkSession:
    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        return active_session
    return SparkSession.builder.getOrCreate()


def _get_config(key: str, default: Optional[str] = None) -> str:
    spark_session = _spark_session()
    if default is None:
        return spark_session.conf.get(key)
    return spark_session.conf.get(key, default)


ENVIRONMENT = _get_config(CONFIG_KEYS["environment"], "dev")
RESOURCE_PREFIX = _get_config(CONFIG_KEYS["resource_prefix"], "")
SOURCE_TABLE = _get_config(CONFIG_KEYS["source_table"], "samples.bakehouse.sales_customers")
TARGET_CATALOG = _get_config(CONFIG_KEYS["target_catalog"], "workspace")
TARGET_SCHEMA = _get_config(CONFIG_KEYS["target_schema"], "default")
TARGET_BASE_PATH = _get_config(CONFIG_KEYS["target_base_path"], "")

# Column used to order CDC events for SCD2 sequencing (must be monotonically increasing per key)
SEQUENCE_COLUMN = _get_config(CONFIG_KEYS["sequence_column"], "ingestion_ts")

# Columns excluded from SCD2 history tracking.
# ingestion_ts and record_hash change every batch; including them would create spurious new versions.
TRACK_HISTORY_EXCEPT_COLUMNS = [
    column_name.strip()
    for column_name in _get_config(
        CONFIG_KEYS["history_exclusions"],
        "ingestion_ts,record_hash,environment_name,source_table_name",
    ).split(",")
    if column_name.strip()
]


def layer_config(layer: str, key: str, default: Optional[str] = None) -> str:
    cfg_key = CONFIG_KEYS[f"{layer}_{key}"]
    return _get_config(cfg_key, default)


def dataset_name(dataset_key: str) -> str:
    dataset_basename = f"{RESOURCE_PREFIX}{DATASET_BASE_NAMES[dataset_key]}"
    layer = DATASET_LAYERS[dataset_key]
    catalog = layer_config(layer, "catalog", TARGET_CATALOG)
    schema = layer_config(layer, "schema", TARGET_SCHEMA)
    return f"{catalog}.{schema}.{dataset_basename}"


DATASET_NAMES = {
    dataset_key: dataset_name(dataset_key)
    for dataset_key in DATASET_BASE_NAMES
}


def dataset_path(dataset_key: str) -> Optional[str]:
    layer = DATASET_LAYERS[dataset_key]
    volume = layer_config(layer, "volume", TARGET_BASE_PATH)
    if not volume:
        return None
    dataset_basename = f"{RESOURCE_PREFIX}{DATASET_BASE_NAMES[dataset_key]}"
    return f"{volume.rstrip('/')}/{dataset_basename}"


def dataset_options(
    dataset_key: str,
    comment: str,
    cluster_by: Optional[List[str]] = None,
) -> Dict[str, object]:
    layer = DATASET_LAYERS[dataset_key]
    options: Dict[str, object] = {
        "name": DATASET_NAMES[dataset_key],
        "comment": comment,
        "table_properties": {
            "quality": layer,
            "project.environment": ENVIRONMENT,
            "project.source.table": SOURCE_TABLE,
        },
    }

    output_path = dataset_path(dataset_key)
    if output_path:
        options["path"] = output_path

    if cluster_by:
        options["cluster_by"] = cluster_by

    return options
