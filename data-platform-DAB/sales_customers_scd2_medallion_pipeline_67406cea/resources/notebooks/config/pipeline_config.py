from typing import Dict, List, Optional

from pyspark.sql import SparkSession


CONFIG_KEYS = {
    # Environment-wide config (from variables.yml + databricks.yml targets)
    "environment": "project.environment",
    "source_table": "project.source.table",
    # Generic fallback output config (overridden per-layer when layer keys are set)
    "target_catalog": "project.target.catalog",
    "target_schema": "project.target.schema",
    "target_base_path": "project.target.base_path",
    # Layer schemas (from variables.yml bronze/silver/gold sections)
    "bronze_schema": "bronze.target.schema",
    "silver_schema": "silver.target.schema",
    "gold_schema": "gold.target.schema",
    # SCD2 & misc
    "history_exclusions": "project.scd2.track_history_except",
}


def _spark_session() -> SparkSession:
    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        return active_session
    return SparkSession.builder.getOrCreate()


def _get_config(key: str, default: Optional[str] = None) -> str:
    spark_session = _spark_session()
    try:
        if default is None:
            return spark_session.conf.get(key)
        return spark_session.conf.get(key, default)
    except Exception:
        if default is None:
            raise
        return default


# Values sourced from bundle variables via pipeline YAML configuration.
# No hardcoded defaults — the pipeline YAML always sets these from ${var.*}.
ENVIRONMENT = _get_config(CONFIG_KEYS["environment"])
SOURCE_TABLE = _get_config(CONFIG_KEYS["source_table"])
TARGET_CATALOG = _get_config(CONFIG_KEYS["target_catalog"])
TARGET_SCHEMA = _get_config(CONFIG_KEYS["target_schema"])
TARGET_BASE_PATH = _get_config(CONFIG_KEYS["target_base_path"], "")

BRONZE_SCHEMA = _get_config(CONFIG_KEYS["bronze_schema"])
SILVER_SCHEMA = _get_config(CONFIG_KEYS["silver_schema"])
GOLD_SCHEMA = _get_config(CONFIG_KEYS["gold_schema"])

TRACK_HISTORY_EXCEPT_COLUMNS = [
    column_name.strip()
    for column_name in _get_config(
        CONFIG_KEYS["history_exclusions"],
        "snapshot_ts,record_hash,environment_name,source_table_name",
    ).split(",")
    if column_name.strip()
]


def dataset_options(
    name: str,
    comment: str,
    quality: str,
    cluster_by: Optional[List[str]] = None,
    path: Optional[str] = None,
) -> Dict[str, object]:
    options: Dict[str, object] = {
        "name": name,
        "comment": comment,
        "table_properties": {
            "quality": quality,
            "project.environment": ENVIRONMENT,
            "project.source.table": SOURCE_TABLE,
        },
    }

    if path:
        options["path"] = path

    if cluster_by:
        options["cluster_by"] = cluster_by

    return options