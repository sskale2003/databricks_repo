"""
SDP-compatible entry point for the Data Security Pipeline.
Executes security controls as module-level side effects (spark.sql DDL),
then exposes a @dp.materialized_view that returns a status DataFrame.
"""

from pyspark import pipelines as dp
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.tags import TagPolicy, Value
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
    tag_str = ", ".join([f"'{k}' = '{v}'" for k, v in tags.items()])
    sql = f"ALTER TABLE {table} SET TAGS ({tag_str})"
    try:
        spark.sql(sql)
        results.append(("table_tag", table, "OK", "", execution_timestamp, pipeline_name, pipeline_run_id))
    except Exception as e:
        results.append(("table_tag", table, "FAIL", str(e), execution_timestamp, pipeline_name, pipeline_run_id))

for col_tag in tag_apps.get("column_tags", []):
    table = col_tag["table"]
    column = col_tag["column"]
    tags = col_tag["tags"]
    for tag_key, tag_value in tags.items():
        sql = f"ALTER TABLE {table} ALTER COLUMN {column} SET TAGS ('{tag_key}' = '{tag_value}')"
        try:
            spark.sql(sql)
            results.append(("column_tag", f"{table}.{column}", "OK", "", execution_timestamp, pipeline_name, pipeline_run_id))
        except Exception as e:
            results.append(("column_tag", f"{table}.{column}", "FAIL", str(e), execution_timestamp, pipeline_name, pipeline_run_id))

# --- Step 3: Create UDFs ---
for udf_def in pipeline_config["udfs"]:
    udf_name = udf_def["full_name"]
    language = udf_def.get("language", "python").upper()
    return_type = udf_def["return_type"]
    comment = udf_def.get("comment", "")
    code_body = udf_def["code"]
    params = udf_def.get("params", [])
    param_str = ", ".join([f"{p['name']} {p['type']}" for p in params])
    sql = f"CREATE OR REPLACE FUNCTION {udf_name}({param_str})\n"
    sql += f"RETURNS {return_type}\n"
    sql += f"LANGUAGE {language}\n"
    if comment:
        sql += f"COMMENT '{escape_sql_string(comment)}'\n"
    if language == "SQL":
        if code_body.strip().upper().startswith("RETURN"):
            sql += code_body
        else:
            sql += f"RETURN {code_body}"
    else:
        sql += f"AS $$\n{code_body}\n$$"
    try:
        spark.sql(sql)
        results.append(("udf", udf_name, "OK", "", execution_timestamp, pipeline_name, pipeline_run_id))
    except Exception as e:
        results.append(("udf", udf_name, "FAIL", str(e), execution_timestamp, pipeline_name, pipeline_run_id))

# --- Step 4: Apply RBAC Privileges (placeholder) ---
# --- Step 5: Apply Manual Row Filters ---
for rf in pipeline_config.get("row_filters", []):
    table = rf["table"]
    udf = rf["udf"]
    using_cols = rf.get("using_columns", [])
    if using_cols:
        cols_str = ", ".join(using_cols)
        sql = f"ALTER TABLE {table} SET ROW FILTER {udf} ON ({cols_str})"
    else:
        sql = f"ALTER TABLE {table} SET ROW FILTER {udf}"
    try:
        spark.sql(sql)
        results.append(("row_filter", table, "OK", "", execution_timestamp, pipeline_name, pipeline_run_id))
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
        sql = f"ALTER TABLE {table} ALTER COLUMN {column} SET MASK {udf} USING COLUMNS ({cols_str})"
    else:
        sql = f"ALTER TABLE {table} ALTER COLUMN {column} SET MASK {udf}"
    try:
        spark.sql(sql)
        results.append(("column_mask", f"{table}.{column}", "OK", "", execution_timestamp, pipeline_name, pipeline_run_id))
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
    sql = f"CREATE POLICY {name}\n"
    sql += f"ON {scope_type} {scope}\n"
    if policy_type == "ROW_FILTER":
        sql += f"ROW FILTER {udf}\n"
    elif policy_type == "COLUMN_MASK":
        sql += f"COLUMN MASK {udf}\n"
    if to_principals:
        to_str = ", ".join([f"`{p}`" for p in to_principals])
        sql += f"TO {to_str}\n"
    if except_principals:
        except_str = ", ".join([f"`{p}`" for p in except_principals])
        sql += f"EXCEPT {except_str}\n"
    sql += "FOR TABLES\n"
    if when_cond:
        sql += f"WHEN {when_cond}\n"
    if match_cols:
        sql += f"MATCH COLUMNS {match_cols}\n"
    if policy_type == "COLUMN_MASK" and on_column:
        sql += f"ON COLUMN {on_column}\n"
    if using_cols:
        if policy_type == "COLUMN_MASK" and on_column:
            additional_cols = [c for c in using_cols if c != on_column]
            if additional_cols:
                using_str = ", ".join(additional_cols)
                sql += f"USING COLUMNS ({using_str})\n"
        else:
            using_str = ", ".join(using_cols)
            sql += f"USING COLUMNS ({using_str})\n"
    try:
        spark.sql(sql)
        results.append(("abac_policy", name, "OK", "", execution_timestamp, pipeline_name, pipeline_run_id))
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
