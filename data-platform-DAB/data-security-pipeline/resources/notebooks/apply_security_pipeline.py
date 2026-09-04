"""
SDP-compatible entry point for the Data Security Pipeline.
Wraps all security application logic in a @dp.materialized_view so the
Spark Declarative Pipeline has a valid table definition.

The function applies security controls as side effects (spark.sql DDL),
then returns a status DataFrame so SDP has a valid materialized view.
"""

from pyspark import pipelines as dp
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.tags import TagPolicy, Value
import json
import os


@dp.materialized_view(
    name="security_application_status",
    comment="Status of data security controls applied by the data security pipeline",
)
def security_application_status():
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

    # --- Step 1: Create Governed Tags ---
    w = WorkspaceClient()
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
            results.append(("governed_tag", tag_key, "OK", ""))
        except Exception as e:
            err_str = str(e)
            if "ALREADY_EXISTS" in err_str.upper() or "already" in err_str.lower():
                try:
                    w.tag_policies.update_tag_policy(
                        tag_policy=TagPolicy(
                            tag_key=tag_key,
                            description=tag_comment,
                            values=[Value(name=v) for v in tag_values],
                        )
                    )
                    results.append(("governed_tag", tag_key, "OK", "updated existing"))
                except Exception as e2:
                    results.append(("governed_tag", tag_key, "FAIL", str(e2)))
            else:
                results.append(("governed_tag", tag_key, "FAIL", str(e)))

    # --- Step 2: Apply Tags to Tables and Columns ---
    tag_apps = pipeline_config["tag_applications"]
    for table_tag in tag_apps.get("table_tags", []):
        table = table_tag["table"]
        tags = table_tag["tags"]
        tag_str = ", ".join([f"'{k}' = '{v}'" for k, v in tags.items()])
        sql = f"ALTER TABLE {table} SET TAGS ({tag_str})"
        try:
            spark.sql(sql)
            results.append(("table_tag", table, "OK", ""))
        except Exception as e:
            results.append(("table_tag", table, "FAIL", str(e)))

    for col_tag in tag_apps.get("column_tags", []):
        table = col_tag["table"]
        column = col_tag["column"]
        tags = col_tag["tags"]
        for tag_key, tag_value in tags.items():
            sql = f"ALTER TABLE {table} ALTER COLUMN {column} SET TAGS ('{tag_key}' = '{tag_value}')"
            try:
                spark.sql(sql)
                results.append(("column_tag", f"{table}.{column}", "OK", ""))
            except Exception as e:
                results.append(("column_tag", f"{table}.{column}", "FAIL", str(e)))

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
            sql += f"COMMENT '{comment}'\n"
        if language == "SQL":
            if code_body.strip().upper().startswith("RETURN"):
                sql += code_body
            else:
                sql += f"RETURN {code_body}"
        else:
            sql += f"AS $$\n{code_body}\n$$"
        try:
            spark.sql(sql)
            results.append(("udf", udf_name, "OK", ""))
        except Exception as e:
            results.append(("udf", udf_name, "FAIL", str(e)))

    # --- Step 4: Apply RBAC Grants ---
    rbac_config = pipeline_config["rbac"]
    for grant in rbac_config.get("grants", []):
        principal = grant["principal"]
        privilege = grant["privilege"]
        object_type = grant["object_type"]
        obj = grant["object"]
        sql = f"GRANT {privilege} ON {object_type} {obj} TO `{principal}`"
        try:
            spark.sql(sql)
            results.append(("rbac", f"{obj}->{principal}", "OK", ""))
        except Exception as e:
            results.append(("rbac", f"{obj}->{principal}", "FAIL", str(e)))

    # --- Step 5: Apply Manual Row Filters ---
    for rf in pipeline_config.get("row_filters", []):
        table = rf["table"]
        udf = rf["udf"]
        using_cols = rf.get("using_columns", [])
        cols_str = ", ".join(using_cols) if using_cols else ""
        sql = f"ALTER TABLE {table} SET ROW FILTER {udf}({cols_str})"
        try:
            spark.sql(sql)
            results.append(("row_filter", table, "OK", ""))
        except Exception as e:
            results.append(("row_filter", table, "FAIL", str(e)))

    # --- Step 6: Apply Manual Column Masks ---
    for cm in pipeline_config.get("column_masks", []):
        table = cm["table"]
        column = cm["column"]
        udf = cm["udf"]
        using_cols = cm.get("using_columns", [column])
        cols_str = ", ".join(using_cols) if using_cols else column
        sql = f"ALTER TABLE {table} ALTER COLUMN {column} SET MASK {udf}({cols_str})"
        try:
            spark.sql(sql)
            results.append(("column_mask", f"{table}.{column}", "OK", ""))
        except Exception as e:
            results.append(("column_mask", f"{table}.{column}", "FAIL", str(e)))

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
        comment = policy.get("comment", "")
        sql = f"CREATE OR REPLACE POLICY {name}\n"
        sql += f"ON {scope_type} {scope}\n"
        if comment:
            sql += f"COMMENT '{comment}'\n"
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
            using_str = ", ".join(using_cols)
            sql += f"USING COLUMNS ({using_str})\n"
        try:
            spark.sql(sql)
            results.append(("abac_policy", name, "OK", ""))
        except Exception as e:
            results.append(("abac_policy", name, "FAIL", str(e)))

    # --- Return status DataFrame ---
    if results:
        return spark.createDataFrame(
            results,
            schema="step STRING, target STRING, status STRING, message STRING",
        )
    else:
        return spark.sql("SELECT 'none' AS step, 'none' AS target, 'OK' AS status, 'No security controls configured' AS message")
