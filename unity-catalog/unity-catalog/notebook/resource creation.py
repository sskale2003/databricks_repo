# Databricks notebook source
# DBTITLE 1,Volume setup notes
# MAGIC %md
# MAGIC # Unity Catalog Medallion Resource Setup
# MAGIC
# MAGIC This notebook creates a rerunnable Unity Catalog medallion layout using the standard `bronze`, `silver`, and `gold` schemas in the `dev` catalog. It also creates standard managed volumes for each layer (`data` and `checkpoints`) and verifies the resulting schemas and volumes after execution.

# COMMAND ----------

# DBTITLE 1,Create catalog schema and volume
import re

catalog_name = "dev"
layer_definitions = {
    "bronze": {
        "schema_purpose": "Raw landing layer for source-aligned data.",
        "volumes": {
            "data": "Landing files for the bronze layer.",
            "checkpoints": "Streaming and Auto Loader checkpoints for the bronze layer.",
        },
    },
    "silver": {
        "schema_purpose": "Validated and conformed layer for cleaned datasets.",
        "volumes": {
            "data": "Working files for the silver layer.",
            "checkpoints": "Streaming and incremental processing checkpoints for the silver layer.",
        },
    },
    "gold": {
        "schema_purpose": "Serving layer for curated business-ready data products.",
        "volumes": {
            "data": "Business-facing extracts and supporting files for the gold layer.",
            "checkpoints": "Checkpoint storage for gold-layer refresh jobs.",
        },
    },
}

identifier_pattern = re.compile(r"^[a-z][a-z0-9_]*$")


def validate_identifier(name: str, label: str) -> None:
    if not identifier_pattern.fullmatch(name):
        raise ValueError(
            f"Invalid {label} '{name}'. Use lowercase letters, numbers, and underscores only, and start with a letter."
        )


def execute_ddl(statement: str, object_type: str, object_name: str, results: list) -> bool:
    try:
        spark.sql(statement)
        results.append(
            {
                "object_type": object_type,
                "object_name": object_name,
                "status": "success",
                "statement": statement,
                "message": f"Ensured {object_type} {object_name} exists.",
            }
        )
        return True
    except Exception as exc:
        results.append(
            {
                "object_type": object_type,
                "object_name": object_name,
                "status": "error",
                "statement": statement,
                "message": str(exc),
            }
        )
        return False


validate_identifier(catalog_name, "catalog name")
for layer_name, layer_spec in layer_definitions.items():
    validate_identifier(layer_name, "schema name")
    for volume_name in layer_spec["volumes"]:
        validate_identifier(volume_name, "volume name")

execution_results = []

catalog_created = execute_ddl(
    statement=f"CREATE CATALOG IF NOT EXISTS {catalog_name}",
    object_type="catalog",
    object_name=catalog_name,
    results=execution_results,
)

if catalog_created:
    for layer_name, layer_spec in layer_definitions.items():
        schema_name = f"{catalog_name}.{layer_name}"
        schema_created = execute_ddl(
            statement=f"CREATE SCHEMA IF NOT EXISTS {schema_name}",
            object_type="schema",
            object_name=schema_name,
            results=execution_results,
        )

        if not schema_created:
            continue

        for volume_name in layer_spec["volumes"]:
            execute_ddl(
                statement=f"CREATE VOLUME IF NOT EXISTS {schema_name}.{volume_name}",
                object_type="volume",
                object_name=f"{schema_name}.{volume_name}",
                results=execution_results,
            )

execution_log_df = spark.createDataFrame(execution_results)
display(execution_log_df.orderBy("object_type", "object_name"))

schema_validation_rows = []

try:
    schema_rows = spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
    for row in schema_rows:
        row_dict = row.asDict()
        schema_value = (
            row_dict.get("namespace")
            or row_dict.get("databaseName")
            or row_dict.get("database_name")
            or next(iter(row_dict.values()))
        )
        if schema_value in layer_definitions:
            schema_validation_rows.append(
                {
                    "catalog_name": catalog_name,
                    "schema_name": schema_value,
                    "expected": "yes",
                    "found": "yes",
                }
            )
except Exception as exc:
    schema_validation_rows.append(
        {
            "catalog_name": catalog_name,
            "schema_name": None,
            "expected": "bronze,silver,gold",
            "found": f"error: {str(exc)}",
        }
    )

expected_schema_names = set(layer_definitions.keys())
found_schema_names = {row["schema_name"] for row in schema_validation_rows if row["schema_name"]}

for missing_schema in sorted(expected_schema_names - found_schema_names):
    schema_validation_rows.append(
        {
            "catalog_name": catalog_name,
            "schema_name": missing_schema,
            "expected": "yes",
            "found": "no",
        }
    )

schema_validation_df = spark.createDataFrame(schema_validation_rows)
display(schema_validation_df.orderBy("schema_name"))

volume_validation_rows = []

for layer_name, layer_spec in layer_definitions.items():
    expected_volumes = set(layer_spec["volumes"].keys())
    discovered_volumes = set()

    try:
        volume_rows = spark.sql(f"SHOW VOLUMES IN {catalog_name}.{layer_name}").collect()
        for row in volume_rows:
            row_dict = row.asDict()
            volume_value = (
                row_dict.get("volume_name")
                or row_dict.get("volume")
                or row_dict.get("name")
                or next(iter(row_dict.values()))
            )
            volume_type = row_dict.get("volume_type") or row_dict.get("type") or "unknown"

            if volume_value in expected_volumes:
                discovered_volumes.add(volume_value)
                volume_validation_rows.append(
                    {
                        "catalog_name": catalog_name,
                        "schema_name": layer_name,
                        "volume_name": volume_value,
                        "volume_type": volume_type,
                        "expected": "yes",
                        "found": "yes",
                    }
                )
    except Exception as exc:
        volume_validation_rows.append(
            {
                "catalog_name": catalog_name,
                "schema_name": layer_name,
                "volume_name": None,
                "volume_type": "error",
                "expected": ",".join(sorted(expected_volumes)),
                "found": str(exc),
            }
        )
        continue

    for missing_volume in sorted(expected_volumes - discovered_volumes):
        volume_validation_rows.append(
            {
                "catalog_name": catalog_name,
                "schema_name": layer_name,
                "volume_name": missing_volume,
                "volume_type": "not_found",
                "expected": "yes",
                "found": "no",
            }
        )

volume_validation_df = spark.createDataFrame(volume_validation_rows)
display(volume_validation_df.orderBy("schema_name", "volume_name"))
