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

# Helper function to format API calls for audit
def format_api_call(method, params):
    """Format API call as a string for audit purposes."""
    return f"API: {method}({', '.join(f'{k}={v}' for k, v in params.items())})"

# Helper to ensure message is never blank
def ensure_message(msg, default="success"):
    """Return message if non-empty, otherwise return default."""
    return msg if msg and msg.strip() else default

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
    _api_call = f"API: WorkspaceClient.groups.create(display_name='{group_name}')"
    try:
        w.groups.create(display_name=group_name)
        results.append(("group", group_name, "OK", "Group created successfully", execution_timestamp, pipeline_name, _api_call))
    except Exception as e:
        err_str = str(e)
        if "already" in err_str.lower() or "ALREADY_EXISTS" in err_str.upper():
            results.append(("group", group_name, "OK", "Group already exists, no changes needed", execution_timestamp, pipeline_name, _api_call))
        else:
            results.append(("group", group_name, "FAIL", f"Failed to create group: {str(e)}", execution_timestamp, pipeline_name, _api_call))

# --- Step 1: Create Governed Tags ---
for tag_def in pipeline_config["governed_tags"]:
    tag_key = tag_def["key"]
    tag_comment = tag_def.get("comment", "")
    tag_values = tag_def.get("values", [])
    _api_call = f"API: WorkspaceClient.tag_policies.create_tag_policy(tag_key='{tag_key}', values={tag_values})"
    try:
        w.tag_policies.create_tag_policy(
            tag_policy=TagPolicy(
                tag_key=tag_key,
                description=tag_comment,
                values=[Value(name=v) for v in tag_values],
            )
        )
        results.append(("governed_tag", tag_key, "OK", f"Governed tag '{tag_key}' created successfully", execution_timestamp, pipeline_name, _api_call))
    except Exception as e:
        err_str = str(e)
        if "ALREADY_EXISTS" in err_str.upper() or "already" in err_str.lower():
            _api_call_update = f"API: WorkspaceClient.tag_policies.update_tag_policy(tag_key='{tag_key}', values={tag_values})"
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
                results.append(("governed_tag", tag_key, "OK", f"Governed tag '{tag_key}' already exists, updated successfully", execution_timestamp, pipeline_name, _api_call_update))
            except Exception as e2:
                results.append(("governed_tag", tag_key, "FAIL", f"Failed to update existing tag: {str(e2)}", execution_timestamp, pipeline_name, _api_call_update))
        else:
            results.append(("governed_tag", tag_key, "FAIL", f"Failed to create governed tag: {str(e)}", execution_timestamp, pipeline_name, _api_call))

# --- Step 2: Apply Tags to Tables and Columns ---
tag_apps = pipeline_config["tag_applications"]
for table_tag in tag_apps.get("table_tags", []):
    table = table_tag["table"]
    tags = table_tag["tags"]
    for tag_key, tag_value in tags.items():
        _api_call = f"API: WorkspaceClient.entity_tag_assignments.create(entity_name='{table}', entity_type='tables', tag_key='{tag_key}', tag_value='{tag_value}')"
        try:
            w.entity_tag_assignments.create(
                tag_assignment=EntityTagAssignment(
                    entity_name=table,
                    entity_type="tables",
                    tag_key=tag_key,
                    tag_value=tag_value,
                )
            )
            results.append(("table_tag", f"{table}.{tag_key}", "OK", f"Tag '{tag_key}={tag_value}' applied to table '{table}'", execution_timestamp, pipeline_name, _api_call))
        except Exception as e:
            err_str = str(e)
            if "already exists" in err_str.lower():
                results.append(("table_tag", f"{table}.{tag_key}", "OK", f"Tag '{tag_key}={tag_value}' already exists on table '{table}'", execution_timestamp, pipeline_name, _api_call))
            else:
                results.append(("table_tag", f"{table}.{tag_key}", "FAIL", f"Failed to apply tag: {str(e)}", execution_timestamp, pipeline_name, _api_call))

for col_tag in tag_apps.get("column_tags", []):
    table = col_tag["table"]
    column = col_tag["column"]
    tags = col_tag["tags"]
    for tag_key, tag_value in tags.items():
        _api_call = f"API: WorkspaceClient.entity_tag_assignments.create(entity_name='{table}.{column}', entity_type='columns', tag_key='{tag_key}', tag_value='{tag_value}')"
        try:
            w.entity_tag_assignments.create(
                tag_assignment=EntityTagAssignment(
                    entity_name=f"{table}.{column}",
                    entity_type="columns",
                    tag_key=tag_key,
                    tag_value=tag_value,
                )
            )
            results.append(("column_tag", f"{table}.{column}.{tag_key}", "OK", f"Tag '{tag_key}={tag_value}' applied to column '{table}.{column}'", execution_timestamp, pipeline_name, _api_call))
        except Exception as e:
            err_str = str(e)
            if "already exists" in err_str.lower():
                results.append(("column_tag", f"{table}.{column}.{tag_key}", "OK", f"Tag '{tag_key}={tag_value}' already exists on column '{table}.{column}'", execution_timestamp, pipeline_name, _api_call))
            else:
                results.append(("column_tag", f"{table}.{column}.{tag_key}", "FAIL", f"Failed to apply column tag: {str(e)}", execution_timestamp, pipeline_name, _api_call))

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
    _external_language = language.lower() if language != "SQL" else None
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
                external_language=_external_language,
            )
        )
        _api_call = f"API: WorkspaceClient.functions.create(name='{udf_name}', language='{language}', return_type='{return_type}')"
        results.append(("udf", udf_name, "OK", f"UDF '{udf_name}' created successfully with language {language}", execution_timestamp, pipeline_name, _api_call))
    except Exception as e:
        _api_call = f"API: WorkspaceClient.functions.create(name='{udf_name}', language='{language}', return_type='{return_type}')"
        err_str = str(e)
        if "already exists" in err_str.lower():
            results.append(("udf", udf_name, "OK", f"UDF '{udf_name}' already exists, no changes needed", execution_timestamp, pipeline_name, _api_call))
        else:
            results.append(("udf", udf_name, "FAIL", f"Failed to create UDF: {str(e)}", execution_timestamp, pipeline_name, _api_call))

# --- Step 4: Apply RBAC Privileges via SQL ---
rbac_config = pipeline_config["rbac"]
_rbac_kw = pipeline_config.get("sql_keywords", {}).get("privilege_grant", "")
for item in rbac_config.get("grants", []):
    principal = item["principal"]
    privilege = item["privilege"]
    object_type = item["object_type"]
    obj = item["object"]
    _ddl = f"{_rbac_kw} {privilege} ON {object_type} {obj} TO `{principal}`"
    try:
        if _warehouse_id:
            w.statement_execution.execute_statement(
                statement=_ddl,
                warehouse_id=_warehouse_id,
                wait_timeout="30s",
            )
            results.append(("rbac", f"{obj}->{principal}", "OK", f"RBAC privilege '{privilege}' granted on {object_type} '{obj}' to principal '{principal}'", execution_timestamp, pipeline_name, _ddl))
        else:
            results.append(("rbac", f"{obj}->{principal}", "FAIL", "No SQL warehouse available for executing RBAC grant", execution_timestamp, pipeline_name, _ddl))
    except Exception as e:
        err_str = str(e)
        if "already" in err_str.lower():
            results.append(("rbac", f"{obj}->{principal}", "OK", f"RBAC privilege '{privilege}' already granted to '{principal}'", execution_timestamp, pipeline_name, _ddl))
        else:
            results.append(("rbac", f"{obj}->{principal}", "FAIL", f"Failed to grant RBAC privilege: {str(e)}", execution_timestamp, pipeline_name, _ddl))

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
            results.append(("row_filter", table, "OK", f"Row filter '{udf}' applied to table '{table}'", execution_timestamp, pipeline_name, _ddl))
        else:
            results.append(("row_filter", table, "FAIL", "No SQL warehouse available for executing row filter DDL", execution_timestamp, pipeline_name, _ddl))
    except Exception as e:
        results.append(("row_filter", table, "FAIL", f"Failed to apply row filter: {str(e)}", execution_timestamp, pipeline_name, _ddl))

# --- Step 6: Apply Manual Column Masks ---
# IMPORTANT: Column masks may not apply if the user has ownership or elevated privileges
# that bypass the mask. Review ABAC policies and ensure proper principal targeting.
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
            results.append(("column_mask", f"{table}.{column}", "OK", f"Column mask '{udf}' applied to column '{table}.{column}'. Note: mask may not apply to table owners or users with bypass privileges.", execution_timestamp, pipeline_name, _ddl))
        else:
            results.append(("column_mask", f"{table}.{column}", "FAIL", "No SQL warehouse available for executing column mask DDL", execution_timestamp, pipeline_name, _ddl))
    except Exception as e:
        results.append(("column_mask", f"{table}.{column}", "FAIL", f"Failed to apply column mask: {str(e)}", execution_timestamp, pipeline_name, _ddl))

# --- Step 7: Apply ABAC Policies ---
# NOTE: ABAC policies require a paid Databricks account (Premium or Enterprise tier)
# Free/Standard accounts should rely on traditional column masking (Step 6) with
# conditional UDFs that check group membership using is_account_group_member()
#
# TROUBLESHOOTING Column Masking:
# If a user sees unmasked data when they shouldn't:
# 1. Check if user is table/catalog/schema OWNER (owners bypass ALL masks)
# 2. Verify the masking UDF includes is_account_group_member() check for admins
# 3. Confirm user is NOT in the 'admins' group
# 4. Test the mask UDF directly: SELECT catalog.schema.mask_udf(column) FROM table
# 5. Check table ownership: DESCRIBE TABLE EXTENDED catalog.schema.table
#
# ABAC POLICIES ARE SKIPPED ON FREE ACCOUNTS - Using traditional column masks instead
abac_policies = pipeline_config.get("abac_policies", [])
if not abac_policies:
    results.append(("abac_policy", "N/A", "OK", "No ABAC policies configured (not available on free tier)", execution_timestamp, pipeline_name, "N/A"))
else:
    results.append(("abac_policy", "N/A", "OK", f"Skipping {len(abac_policies)} ABAC policies (requires Premium/Enterprise tier). Using traditional column masks instead.", execution_timestamp, pipeline_name, "N/A"))

# Uncomment below to enable ABAC policies if you upgrade to Premium/Enterprise
# for policy in abac_policies:
#     name = policy["name"]
#     scope_type = policy["scope_type"]
#     scope = policy["scope"]
#     policy_type = policy["policy_type"]
#     udf = policy["udf"]
#     to_principals = policy.get("to_principals", [])
#     except_principals = policy.get("except_principals", [])
#     when_cond = policy.get("when_condition")
#     match_cols = policy.get("match_columns")
#     on_column = policy.get("on_column")
#     using_cols = policy.get("using_columns", [])
#     _ddl = f"CREATE POLICY {name}\n"
#     _ddl += f"ON {scope_type} {scope}\n"
#     if policy_type == "ROW_FILTER":
#         _ddl += f"ROW FILTER {udf}\n"
#     elif policy_type == "COLUMN_MASK":
#         _ddl += f"COLUMN MASK {udf}\n"
#     if to_principals:
#         to_str = ", ".join([f"`{p}`" for p in to_principals])
#         _ddl += f"TO {to_str}\n"
#     if except_principals:
#         except_str = ", ".join([f"`{p}`" for p in except_principals])
#         _ddl += f"EXCEPT {except_str}\n"
#     _ddl += "FOR TABLES\n"
#     if when_cond:
#         _ddl += f"WHEN {when_cond}\n"
#     if match_cols:
#         _ddl += f"MATCH COLUMNS {match_cols}\n"
#     if policy_type == "COLUMN_MASK" and on_column:
#         _ddl += f"ON COLUMN {on_column}\n"
#     if using_cols:
#         if policy_type == "COLUMN_MASK" and on_column:
#             additional_cols = [c for c in using_cols if c != on_column]
#             if additional_cols:
#                 using_str = ", ".join(additional_cols)
#                 _ddl += f"USING COLUMNS ({using_str})\n"
#         else:
#             using_str = ", ".join(using_cols)
#             _ddl += f"USING COLUMNS ({using_str})\n"
#     try:
#         if _warehouse_id:
#             w.statement_execution.execute_statement(
#                 statement=_ddl,
#                 warehouse_id=_warehouse_id,
#                 wait_timeout="30s",
#             )
#             _msg = f"ABAC policy '{name}' created successfully for {policy_type} on {scope_type} '{scope}'"
#             if to_principals:
#                 _msg += f" targeting principals: {', '.join(to_principals)}"
#             results.append(("abac_policy", name, "OK", _msg, execution_timestamp, pipeline_name, _ddl))
#         else:
#             results.append(("abac_policy", name, "FAIL", "No SQL warehouse available for executing ABAC policy DDL", execution_timestamp, pipeline_name, _ddl))
#     except Exception as e:
#         err_str = str(e)
#         if "already exists" in err_str.lower() or "ALREADY_EXISTS" in err_str.upper():
#             results.append(("abac_policy", name, "OK", f"ABAC policy '{name}' already exists, no changes needed", execution_timestamp, pipeline_name, _ddl))
#         else:
#             results.append(("abac_policy", name, "FAIL", f"Failed to create ABAC policy: {str(e)}", execution_timestamp, pipeline_name, _ddl))

# --- Materialized view: return status DataFrame ---
@dp.materialized_view(
    name="security_application_status",
    comment="Status of data security controls applied by the data security pipeline with audit trail of executed statements",
)
def security_application_status():
    if results:
        return spark.createDataFrame(
            results,
            schema="step STRING, target STRING, status STRING, message STRING, execution_timestamp STRING, pipeline_name STRING, query_statement STRING",
        )
    else:
        return spark.sql(f"SELECT 'none' AS step, 'none' AS target, 'OK' AS status, 'No security controls configured' AS message, '{execution_timestamp}' AS execution_timestamp, '{pipeline_name}' AS pipeline_name, 'N/A' AS query_statement")
