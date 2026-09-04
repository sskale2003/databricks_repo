"""
SDP-compatible entry point for the Data Security Pipeline.
Executes security controls as module-level side effects (spark.sql DDL),
then exposes a @dp.materialized_view that returns a status DataFrame.
"""

from pyspark import pipelines as dp
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.tags import TagPolicy, Value
from databricks.sdk.service.catalog import (
    EntityTagAssignment, CreateFunction, FunctionParameterInfo, FunctionParameterInfos,
    PolicyInfo, ColumnMaskOptions, RowFilterOptions, MatchColumn, FunctionArgument,
    PermissionsChange, Privilege, ColumnTypeName, CreateFunctionRoutineBody,
    CreateFunctionParameterStyle, CreateFunctionSqlDataAccess, CreateFunctionSecurityType,
    PolicyType, SecurableType
)
from datetime import datetime
import json
import os


def escape_sql_string(s):
    """Escape single quotes for SQL string literals."""
    return s.replace("'", "''")


# --- Load and resolve central config ---
# SDP does not define __file__; resolve config from the working directory
# (pipeline root) or fall back to known deployed paths
config_candidates = [
    os.path.join(os.getcwd(), "resources", "notebooks", "config", "pipeline_config.json"),
    "/Workspace/Development/.bundle/sskale2003@gmail.com/data_security_pipeline/files/resources/notebooks/config/pipeline_config.json",
    "/Workspace/Repos/sskale2003@gmail.com/databricks_repo/data-platform-DAB/data-security-pipeline/resources/notebooks/config/pipeline_config.json",
]
config_path = None
for candidate in config_candidates:
    if os.path.exists(candidate):
        config_path = candidate
        break
if config_path is None:
    raise FileNotFoundError("Could not find pipeline_config.json in any known location")
with open(config_path, "r") as f:
    pipeline_config = json.load(f)

# Validate required config sections
required_sections = [
    "catalog", "dab_variables", "governed_tags", "tag_applications",
    "udfs", "rbac", "row_filters", "abac_policies",
]
missing = [s for s in required_sections if s not in pipeline_config]
if missing:
    raise ValueError(f"Missing required config sections: {missing}")

# Resolve DAB variable placeholders from Spark conf
dab_vars = pipeline_config.pop("dab_variables")
resolved_vars = {}
for var_name, var_config in dab_vars.items():
    spark_conf_key = var_config["spark_conf_key"]
    try:
        resolved_vars[var_name] = spark.conf.get(spark_conf_key)
    except Exception as e:
        raise ValueError(
            f"Could not resolve DAB variable '{var_name}' from Spark conf "
            f"'{spark_conf_key}'. Error: {e}"
        )

# Replace ${var_name} placeholders throughout the config
if resolved_vars:
    config_str = json.dumps(pipeline_config)
    for var_name, var_value in resolved_vars.items():
        config_str = config_str.replace(f"${{{var_name}}}", var_value)
    pipeline_config = json.loads(config_str)

results = []

# Capture audit metadata
try:
    pipeline_run_id = spark.conf.get("spark.databricks.pipeline.updateId")
except Exception:
    pipeline_run_id = "unknown"

try:
    pipeline_name = spark.conf.get("spark.databricks.pipeline.name")
except Exception:
    pipeline_name = "unknown"

execution_timestamp = datetime.utcnow().isoformat()

w = WorkspaceClient()

# Get warehouse ID for DDL via statement execution API (row filters, column masks)
_warehouse_id = None
try:
    _warehouse_id = spark.conf.get("spark.databricks.sql.warehouse.id")
except Exception:
    pass
if not _warehouse_id:
    for _wh in w.warehouses.list():
        if _wh.state == "RUNNING":
            _warehouse_id = _wh.id
            break
    if not _warehouse_id:
        _whs = list(w.warehouses.list())
        if _whs:
            _warehouse_id = _whs[0].id

# --- Step 0: Create Groups ---
for group_def in pipeline_config.get("rbac", {}).get("groups", []):
    group_name = group_def["name"]
    try:
        w.groups.create(display_name=group_name)
        results.append(("group", group_name, "OK", "created", execution_timestamp, pipeline_name, pipeline_run_id))
    except Exception as e:
        err_str = str(e)
        if "already" in err_str.lower() or "ALREADY_EXISTS" in err_str.upper():
            results.append(("group", group_name, "OK", "already exists", execution_timestamp, pipeline_name, pipeline_run_id))
        else:
            results.append(("group", group_name, "FAIL", str(e), execution_timestamp, pipeline_name, pipeline_run_id))

# --- Step 1: Create Governed Tags ---
for tag_def in pipeline_config["governed_tags"]:
    tag_key = tag_def["key"]
    tag_comment = tag_def.get("comment", "")
    tag_values = tag_def.get("values", [])
    try:
        w.tag_policies.create_tag_policy(
            tag_policy=TagPolicy(
                tag_key=tag_key,
                description=tag_comment,
                values=[Value(name=v) for v in tag_values],
            )
        )
        results.append(("governed_tag", tag_key, "OK", "", execution_timestamp, pipeline_name, pipeline_run_id))
    except Exception as e:
        err_str = str(e)
        if "ALREADY_EXISTS" in err_str.upper() or "already" in err_str.lower():
            try:
                w.tag_policies.update_tag_policy(
                    tag_key=tag_key,
                    update_mask="description,values",
                    tag_policy=TagPolicy(
                        tag_key=tag_key,
                        description=tag_comment,
                        values=[Value(name=v) for v in tag_values],
                    )
                )
                results.append(("governed_tag", tag_key, "OK", "updated existing", execution_timestamp, pipeline_name, pipeline_run_id))
            except Exception as e2:
                results.append(("governed_tag", tag_key, "FAIL", str(e2), execution_timestamp, pipeline_name, pipeline_run_id))
        else:
            results.append(("governed_tag", tag_key, "FAIL", str(e), execution_timestamp, pipeline_name, pipeline_run_id))

# --- Step 2: Apply Tags to Tables and Columns ---
tag_apps = pipeline_config["tag_applications"]
for table_tag in tag_apps.get("table_tags", []):
    table = table_tag["table"]
    tags = table_tag["tags"]
    for tag_key, tag_value in tags.items():
        try:
            w.entity_tag_assignments.create(
                tag_assignment=EntityTagAssignment(
                    entity_name=table,
                    entity_type="tables",
                    tag_key=tag_key,
                    tag_value=tag_value,
                )
            )
            results.append(("table_tag", f"{table}.{tag_key}", "OK", "", execution_timestamp, pipeline_name, pipeline_run_id))
        except Exception as e:
            results.append(("table_tag", f"{table}.{tag_key}", "FAIL", str(e), execution_timestamp, pipeline_name, pipeline_run_id))

for col_tag in tag_apps.get("column_tags", []):
    table = col_tag["table"]
    column = col_tag["column"]
    tags = col_tag["tags"]
    for tag_key, tag_value in tags.items():
        try:
            w.entity_tag_assignments.create(
                tag_assignment=EntityTagAssignment(
                    entity_name=f"{table}.{column}",
                    entity_type="columns",
                    tag_key=tag_key,
                    tag_value=tag_value,
                )
            )
            results.append(("column_tag", f"{table}.{column}.{tag_key}", "OK", "", execution_timestamp, pipeline_name, pipeline_run_id))
        except Exception as e:
            results.append(("column_tag", f"{table}.{column}.{tag_key}", "FAIL", str(e), execution_timestamp, pipeline_name, pipeline_run_id))

# --- Step 3: Create UDFs ---
for udf_def in pipeline_config["udfs"]:
    udf_name = udf_def["full_name"]
    language = udf_def.get("language", "python").upper()
    return_type = udf_def["return_type"]
    comment = udf_def.get("comment", "")
    code_body = udf_def["code"]
    params = udf_def.get("params", [])
    # Parse full_name: catalog.schema.function_name
    _parts = udf_name.split(".")
    _catalog = udf_def.get("catalog", _parts[0] if len(_parts) > 2 else "")
    _schema = udf_def.get("schema", _parts[1] if len(_parts) > 2 else "")
    _fn_name = udf_def.get("name", _parts[-1])
    # Map return type to ColumnTypeName enum
    try:
        _data_type = getattr(ColumnTypeName, return_type.upper().replace(" ", "_"))
    except AttributeError:
        _data_type = ColumnTypeName.STRING
    # Build parameter info list
    _param_infos = [
        FunctionParameterInfo(
            name=p["name"],
            type_text=p["type"],
            type_name=getattr(ColumnTypeName, p["type"].upper().replace(" ", "_"), ColumnTypeName.STRING),
            type_json=json.dumps({"name": p["type"].upper().replace(" ", "_"), "type": p["type"].upper().replace(" ", "_")}),
            position=idx,
        )
        for idx, p in enumerate(params)
    ]
    # Set routine body and definition based on language
    if language == "SQL":
        _routine_body = CreateFunctionRoutineBody.SQL
        _routine_def = code_body if code_body.strip().upper().startswith("RETURN") else f"RETURN {code_body}"
    else:
        _routine_body = CreateFunctionRoutineBody.EXTERNAL
        _routine_def = code_body
    try:
        w.functions.create(
            function_info=CreateFunction(
                name=_fn_name,
                catalog_name=_catalog,
                schema_name=_schema,
                input_params=FunctionParameterInfos(parameters=_param_infos) if _param_infos else None,
                data_type=_data_type,
                full_data_type=return_type,
                routine_body=_routine_body,
                routine_definition=_routine_def,
                parameter_style=CreateFunctionParameterStyle.S,
                is_deterministic=False,
                sql_data_access=CreateFunctionSqlDataAccess.NO_SQL,
                is_null_call=True,
                security_type=CreateFunctionSecurityType.DEFINER,
                specific_name=_fn_name,
                comment=comment if comment else None,
            )
        )
        results.append(("udf", udf_name, "OK", "", execution_timestamp, pipeline_name, pipeline_run_id))
    except Exception as e:
        results.append(("udf", udf_name, "FAIL", str(e), execution_timestamp, pipeline_name, pipeline_run_id))

# --- Step 4: Apply RBAC Privileges via SDK ---
rbac_config = pipeline_config["rbac"]
for item in rbac_config.get("grants", []):
    principal = item["principal"]
    privilege = item["privilege"]
    object_type = item["object_type"]
    obj = item["object"]
    try:
        _sec_type = getattr(SecurableType, object_type.upper().replace(" ", "_"), SecurableType.TABLE)
        w.grants.update(
            securable_type=_sec_type,
            full_name=obj,
            changes=[
                PermissionsChange(
                    principal=principal,
                    privilege=privilege,
                )
            ]
        )
        results.append(("rbac", f"{obj}->{principal}", "OK", "", execution_timestamp, pipeline_name, pipeline_run_id))
    except Exception as e:
        results.append(("rbac", f"{obj}->{principal}", "FAIL", str(e), execution_timestamp, pipeline_name, pipeline_run_id))

# --- Step 5: Apply Manual Row Filters ---
for rf in pipeline_config.get("row_filters", []):
    table = rf["table"]
    udf = rf["udf"]
    using_cols = rf.get("using_columns", [])
    if using_cols:
        cols_str = ", ".join(using_cols)
        _ddl = f"ALTER TABLE {table} SET ROW FILTER {udf} ON ({cols_str})"
    else:
        _ddl = f"ALTER TABLE {table} SET ROW FILTER {udf}"
    try:
        if _warehouse_id:
            w.statement_execution.execute_statement(
                statement=_ddl,
                warehouse_id=_warehouse_id,
                wait_timeout="30s",
            )
            results.append(("row_filter", table, "OK", "", execution_timestamp, pipeline_name, pipeline_run_id))
        else:
            results.append(("row_filter", table, "FAIL", "No SQL warehouse available", execution_timestamp, pipeline_name, pipeline_run_id))
    except Exception as e:
        results.append(("row_filter", table, "FAIL", str(e), execution_timestamp, pipeline_name, pipeline_run_id))

# --- Step 6: Apply Manual Column Masks ---
for cm in pipeline_config.get("column_masks", []):
    table = cm["table"]
    column = cm["column"]
    udf = cm["udf"]
    using_cols = cm.get("using_columns", [])
    if using_cols:
        cols_str = ", ".join(using_cols)
        _ddl = f"ALTER TABLE {table} ALTER COLUMN {column} SET MASK {udf} USING COLUMNS ({cols_str})"
    else:
        _ddl = f"ALTER TABLE {table} ALTER COLUMN {column} SET MASK {udf}"
    try:
        if _warehouse_id:
            w.statement_execution.execute_statement(
                statement=_ddl,
                warehouse_id=_warehouse_id,
                wait_timeout="30s",
            )
            results.append(("column_mask", f"{table}.{column}", "OK", "", execution_timestamp, pipeline_name, pipeline_run_id))
        else:
            results.append(("column_mask", f"{table}.{column}", "FAIL", "No SQL warehouse available", execution_timestamp, pipeline_name, pipeline_run_id))
    except Exception as e:
        results.append(("column_mask", f"{table}.{column}", "FAIL", str(e), execution_timestamp, pipeline_name, pipeline_run_id))

# --- Step 7: Apply ABAC Policies ---
for policy in pipeline_config.get("abac_policies", []):
    name = policy["name"]
    scope_type = policy["scope_type"]
    scope = policy["scope"]
    policy_type = policy["policy_type"]
    udf = policy["udf"]
    to_principals = policy.get("to_principals", [])
    except_principals = policy.get("except_principals", [])
    when_cond = policy.get("when_condition")
    match_cols = policy.get("match_columns")
    on_column = policy.get("on_column")
    using_cols = policy.get("using_columns", [])
    _ddl = f"CREATE POLICY {name}\n"
    _ddl += f"ON {scope_type} {scope}\n"
    if policy_type == "ROW_FILTER":
        _ddl += f"ROW FILTER {udf}\n"
    elif policy_type == "COLUMN_MASK":
        _ddl += f"COLUMN MASK {udf}\n"
    if to_principals:
        to_str = ", ".join([f"`{p}`" for p in to_principals])
        _ddl += f"TO {to_str}\n"
    if except_principals:
        except_str = ", ".join([f"`{p}`" for p in except_principals])
        _ddl += f"EXCEPT {except_str}\n"
    _ddl += "FOR TABLES\n"
    if when_cond:
        _ddl += f"WHEN {when_cond}\n"
    if match_cols:
        _ddl += f"MATCH COLUMNS {match_cols}\n"
    if policy_type == "COLUMN_MASK" and on_column:
        _ddl += f"ON COLUMN {on_column}\n"
    if using_cols:
        if policy_type == "COLUMN_MASK" and on_column:
            additional_cols = [c for c in using_cols if c != on_column]
            if additional_cols:
                using_str = ", ".join(additional_cols)
                _ddl += f"USING COLUMNS ({using_str})\n"
        else:
            using_str = ", ".join(using_cols)
            _ddl += f"USING COLUMNS ({using_str})\n"
    try:
        if _warehouse_id:
            w.statement_execution.execute_statement(
                statement=_ddl,
                warehouse_id=_warehouse_id,
                wait_timeout="30s",
            )
            results.append(("abac_policy", name, "OK", "", execution_timestamp, pipeline_name, pipeline_run_id))
        else:
            results.append(("abac_policy", name, "FAIL", "No SQL warehouse available", execution_timestamp, pipeline_name, pipeline_run_id))
    except Exception as e:
        err_str = str(e)
        if "already exists" in err_str.lower() or "ALREADY_EXISTS" in err_str.upper():
            results.append(("abac_policy", name, "OK", "already exists", execution_timestamp, pipeline_name, pipeline_run_id))
        else:
            results.append(("abac_policy", name, "FAIL", str(e), execution_timestamp, pipeline_name, pipeline_run_id))

# --- Materialized view: return status DataFrame ---
@dp.materialized_view(
    name="security_application_status",
    comment="Status of data security controls applied by the data security pipeline",
)
def security_application_status():
    if results:
        return spark.createDataFrame(
            results,
            schema="step STRING, target STRING, status STRING, message STRING, execution_timestamp STRING, pipeline_name STRING, pipeline_run_id STRING",
        )
    else:
        return spark.sql(f"SELECT 'none' AS step, 'none' AS target, 'OK' AS status, 'No security controls configured' AS message, '{execution_timestamp}' AS execution_timestamp, '{pipeline_name}' AS pipeline_name, '{pipeline_run_id}' AS pipeline_run_id")
