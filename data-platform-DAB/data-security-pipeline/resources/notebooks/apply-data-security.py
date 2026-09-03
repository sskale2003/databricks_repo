# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Apply Data Security Pipeline — Overview
# MAGIC %md
# MAGIC # Apply Data Security Pipeline
# MAGIC
# MAGIC This notebook applies comprehensive data security controls in chronological order:
# MAGIC
# MAGIC 1. **Governed Tags** — Create account-level tags for data classification
# MAGIC 2. **Tag Application** — Apply tags to tables and columns
# MAGIC 3. **UDFs** — Create row filter and column mask functions
# MAGIC 4. **RBAC** — Grant role-based access privileges to groups
# MAGIC 5. **Row-Level Security** — Apply manual row filters (`ALTER TABLE SET ROW FILTER`)
# MAGIC 6. **Column-Level Security & Data Masking** — Apply manual column masks (`ALTER TABLE SET MASK`)
# MAGIC 7. **ABAC Policies** — Create attribute-based access control policies (`CREATE POLICY`)
# MAGIC 8. **Verification** — Verify all security controls
# MAGIC
# MAGIC All configuration is sourced from the centralized `config/pipeline_config.json` file.

# COMMAND ----------

# DBTITLE 1,Load and Validate Central Config
import json
import os

# Widget for config file path (overrideable at runtime)
dbutils.widgets.text(
    "config_file_path",
    "/Workspace/Repos/sskale2003@gmail.com/databricks_repo/data-platform-DAB/data-security-pipeline/resources/notebooks/config/pipeline_config.json",
    "Config File Path",
)

config_path = dbutils.widgets.get("config_file_path")

# Load the central configuration
with open(config_path, "r") as f:
    pipeline_config = json.load(f)

# Validate required config sections
required_sections = [
    "catalog",
    "dab_variables",
    "governed_tags",
    "tag_applications",
    "udfs",
    "rbac",
    "row_filters",
    "column_masks",
    "abac_policies",
]
missing = [s for s in required_sections if s not in pipeline_config]
if missing:
    raise ValueError(f"Missing required config sections: {missing}")

# Resolve DAB variable placeholders (${var_name}) from Spark conf
# The config uses ${catalog} and ${gold_schema} which are resolved at runtime
# from DAB bundle variables set via the pipeline YAML.
dab_vars = pipeline_config.pop("dab_variables")
resolved_vars = {}
for var_name, var_config in dab_vars.items():
    spark_conf_key = var_config["spark_conf_key"]
    try:
        resolved_vars[var_name] = spark.conf.get(spark_conf_key)
        print(f"  DAB variable '{var_name}' = '{resolved_vars[var_name]}' (from {spark_conf_key})")
    except Exception as e:
        raise ValueError(
            f"Could not resolve DAB variable '{var_name}' from Spark conf '{spark_conf_key}'. "
            f"Ensure the pipeline YAML sets this from ${{var.*}}. Error: {e}"
        )

# Replace ${var_name} placeholders throughout the config
if resolved_vars:
    config_str = json.dumps(pipeline_config)
    for var_name, var_value in resolved_vars.items():
        config_str = config_str.replace(f"${{{var_name}}}", var_value)
    pipeline_config = json.loads(config_str)
    print(f"  Resolved {len(resolved_vars)} DAB variable(s) in config\n")

# Display config summary
print(f"Pipeline: {pipeline_config['pipeline']['name']} v{pipeline_config['pipeline']['version']}")
print(f"Catalog: {pipeline_config['catalog']['name']}")
print(f"Tables: {len(pipeline_config.get('tables', []))}")
print(f"Governed tags: {len(pipeline_config['governed_tags'])}")
print(f"UDFs: {len(pipeline_config['udfs'])}")
print(f"RBAC grants: {len(pipeline_config['rbac']['grants'])}")
print(f"Manual row filters: {len(pipeline_config['row_filters'])}")
print(f"Manual column masks: {len(pipeline_config['column_masks'])}")
print(f"ABAC policies: {len(pipeline_config['abac_policies'])}")

# COMMAND ----------

# DBTITLE 1,Step 1 — Create Governed Tags
# MAGIC %md
# MAGIC ## Step 1: Create Governed Tags
# MAGIC
# MAGIC Governed tags are account-level tags with enforced allowed values. They are the foundation for ABAC policies and data classification. Tags applied at catalog or schema level inherit to child objects automatically during ABAC policy evaluation.

# COMMAND ----------

# DBTITLE 1,Create Governed Tags
# Step 1: Create Governed Tags
# Governed tags are account-level tags with enforced allowed values.
# They are the foundation for ABAC policies.
# Note: Governed tags cannot be created via spark.sql() DDL — use the Python SDK.

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.tags import TagPolicy, Value

w = WorkspaceClient()

catalog_name = pipeline_config["catalog"]["name"]

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
        print(f"  [OK] Created governed tag: {tag_key}")
    except Exception as e:
        err_str = str(e)
        if "ALREADY_EXISTS" in err_str.upper() or "already" in err_str.lower():
            # Tag already exists — update its values and description
            try:
                w.tag_policies.update_tag_policy(
                    tag_policy=TagPolicy(
                        tag_key=tag_key,
                        description=tag_comment,
                        values=[Value(name=v) for v in tag_values],
                    )
                )
                print(f"  [OK] Updated existing governed tag: {tag_key}")
            except Exception as e2:
                print(f"  [FAIL] Could not create or update tag '{tag_key}': {e2}")
        else:
            print(f"  [FAIL] Could not create tag '{tag_key}': {e}")

print("\n--- Governed tags creation complete ---")

# COMMAND ----------

# DBTITLE 1,Step 2 — Apply Tags to Tables and Columns
# MAGIC %md
# MAGIC ## Step 2: Apply Tags to Tables and Columns
# MAGIC
# MAGIC Tags applied at the table level enable `WHEN` conditions in ABAC policies. Column-level tags enable `MATCH COLUMNS` conditions for row filters and column masks.

# COMMAND ----------

# DBTITLE 1,Apply Tags to Tables and Columns
# Step 2: Apply Tags to Tables and Columns
# Tags enable ABAC policy matching at query time.

tag_apps = pipeline_config["tag_applications"]

# --- Apply table-level tags ---
print("Applying table-level tags...")
for table_tag in tag_apps.get("table_tags", []):
    table = table_tag["table"]
    tags = table_tag["tags"]

    tag_str = ", ".join([f"'{k}' = '{v}'" for k, v in tags.items()])
    sql = f"ALTER TABLE {table} SET TAGS ({tag_str})"

    try:
        spark.sql(sql)
        print(f"  [OK] Tags applied to table: {table} -> {tags}")
    except Exception as e:
        print(f"  [FAIL] Could not tag table {table}: {e}")

# --- Apply column-level tags ---
print("\nApplying column-level tags...")
for col_tag in tag_apps.get("column_tags", []):
    table = col_tag["table"]
    column = col_tag["column"]
    tags = col_tag["tags"]

    for tag_key, tag_value in tags.items():
        sql = f"ALTER TABLE {table} ALTER COLUMN {column} SET TAGS ('{tag_key}' = '{tag_value}')"
        try:
            spark.sql(sql)
            print(f"  [OK] Tag '{tag_key}'='{tag_value}' applied to {table}.{column}")
        except Exception as e:
            print(f"  [FAIL] Could not tag {table}.{column}: {e}")

print("\n--- Tag application complete ---")

# COMMAND ----------

# DBTITLE 1,Step 3 — Create UDFs
# MAGIC %md
# MAGIC ## Step 3: Create UDFs for Row Filters and Column Masks
# MAGIC
# MAGIC Python and SQL UDFs registered in Unity Catalog implement the actual security logic:
# MAGIC - **Row filter UDFs** must return `BOOLEAN` — `true` keeps the row visible
# MAGIC - **Column mask UDFs** must return the same type as the masked column
# MAGIC - SQL UDFs can use built-in functions like `is_member()` for group-based filtering
# MAGIC - Python UDFs are used for complex masking logic (SSN, email, phone, etc.)

# COMMAND ----------

# DBTITLE 1,Create UDFs for Row Filters and Column Masks
# Step 3: Create UDFs for Row Filters and Column Masks
# These UDFs implement the actual filtering and masking logic.
# Row filter UDFs must return BOOLEAN (true = row visible).
# Column mask UDFs must return the same type as the masked column.

for udf_def in pipeline_config["udfs"]:
    udf_name = udf_def["full_name"]
    udf_type = udf_def["type"]
    language = udf_def.get("language", "python").upper()
    return_type = udf_def["return_type"]
    comment = udf_def.get("comment", "")
    code_body = udf_def["code"]
    params = udf_def.get("params", [])

    # Build parameter list
    param_str = ", ".join([f"{p['name']} {p['type']}" for p in params])

    # Build CREATE FUNCTION SQL
    sql = f"CREATE OR REPLACE FUNCTION {udf_name}({param_str})\n"
    sql += f"RETURNS {return_type}\n"
    sql += f"LANGUAGE {language}\n"
    if comment:
        sql += f"COMMENT '{comment}'\n"

    if language == "SQL":
        # SQL UDFs use RETURN expression
        if code_body.strip().upper().startswith("RETURN"):
            sql += code_body
        else:
            sql += f"RETURN {code_body}"
    else:
        # Python UDFs use AS $$ body $$
        sql += f"AS $$\n{code_body}\n$$"

    try:
        spark.sql(sql)
        print(f"  [OK] Created {udf_type} UDF: {udf_name} (LANGUAGE {language})")
    except Exception as e:
        print(f"  [FAIL] Could not create UDF {udf_name}: {e}")

print("\n--- UDF creation complete ---")

# COMMAND ----------

# DBTITLE 1,Step 4 — Apply RBAC Grants
# MAGIC %md
# MAGIC ## Step 4: Apply RBAC (Role-Based Access Control) Grants
# MAGIC
# MAGIC Grant privileges (SELECT, ALL PRIVILEGES, etc.) on catalogs, schemas, and tables to groups. Groups must already exist in the Databricks workspace (created via the account admin console or SCIM API).

# COMMAND ----------

# DBTITLE 1,Apply RBAC Grants
# Step 4: Apply RBAC (Role-Based Access Control) Grants
# Grant privileges on catalogs, schemas, and tables to groups.
# Note: Groups must already exist (created via account admin console or SCIM API).

rbac_config = pipeline_config["rbac"]

# Display configured groups for reference
print("Configured groups:")
for group in rbac_config.get("groups", []):
    print(f"  - {group['name']}: {group.get('comment', '')}")

print("\nApplying RBAC grants...")
for grant in rbac_config.get("grants", []):
    principal = grant["principal"]
    privilege = grant["privilege"]
    object_type = grant["object_type"]
    obj = grant["object"]

    sql = f"GRANT {privilege} ON {object_type} {obj} TO `{principal}`"

    try:
        spark.sql(sql)
        print(f"  [OK] GRANT {privilege} ON {object_type} {obj} TO `{principal}`")
    except Exception as e:
        print(f"  [FAIL] {sql}: {e}")

print("\n--- RBAC grants complete ---")

# COMMAND ----------

# DBTITLE 1,Step 5 — Manual Row-Level Security
# MAGIC %md
# MAGIC ## Step 5: Apply Manual Row-Level Security
# MAGIC
# MAGIC Uses `ALTER TABLE ... SET ROW FILTER` to attach a row filter UDF directly to a table. This is **table-specific** — for tag-driven, dynamic filtering across multiple tables, use ABAC policies (Step 7).

# COMMAND ----------

# DBTITLE 1,Apply Manual Row Filters
# Step 5: Apply Manual Row-Level Security
# Uses ALTER TABLE ... SET ROW FILTER to attach a row filter UDF to a table.
# This is table-specific (not tag-driven like ABAC).

for rf in pipeline_config.get("row_filters", []):
    table = rf["table"]
    udf = rf["udf"]
    using_cols = rf.get("using_columns", [])

    cols_str = ", ".join(using_cols) if using_cols else ""
    sql = f"ALTER TABLE {table} SET ROW FILTER {udf}({cols_str})"

    try:
        spark.sql(sql)
        print(f"  [OK] Row filter applied: {table} -> {udf}({cols_str})")
    except Exception as e:
        print(f"  [FAIL] Row filter on {table}: {e}")

print("\n--- Manual row filters complete ---")

# COMMAND ----------

# DBTITLE 1,Step 6 — Manual Column Masks & Data Masking
# MAGIC %md
# MAGIC ## Step 6: Apply Manual Column-Level Security & Data Masking
# MAGIC
# MAGIC Uses `ALTER TABLE ... ALTER COLUMN ... SET MASK` to attach a column mask UDF directly to a column. For tag-driven, dynamic masking across multiple tables, use ABAC policies (Step 7).

# COMMAND ----------

# DBTITLE 1,Apply Manual Column Masks
# Step 6: Apply Manual Column-Level Security & Data Masking
# Uses ALTER TABLE ... ALTER COLUMN ... SET MASK to attach a column mask UDF.
# This is column-specific (not tag-driven like ABAC).

for cm in pipeline_config.get("column_masks", []):
    table = cm["table"]
    column = cm["column"]
    udf = cm["udf"]
    using_cols = cm.get("using_columns", [column])

    cols_str = ", ".join(using_cols) if using_cols else column
    sql = f"ALTER TABLE {table} ALTER COLUMN {column} SET MASK {udf}({cols_str})"

    try:
        spark.sql(sql)
        print(f"  [OK] Column mask applied: {table}.{column} -> {udf}({cols_str})")
    except Exception as e:
        print(f"  [FAIL] Column mask on {table}.{column}: {e}")

print("\n--- Manual column masks complete ---")

# COMMAND ----------

# DBTITLE 1,Step 7 — ABAC Policies
# MAGIC %md
# MAGIC ## Step 7: Apply ABAC (Attribute-Based Access Control) Policies
# MAGIC
# MAGIC ABAC policies use governed tags to dynamically enforce row filters and column masks across catalogs, schemas, or tables. Unlike manual filters/masks, ABAC policies **automatically apply** to any table matching the tag condition.
# MAGIC
# MAGIC **Row Filter Policy**: `CREATE POLICY ... ROW FILTER ... FOR TABLES WHEN ... MATCH COLUMNS ... USING COLUMNS ...`
# MAGIC
# MAGIC **Column Mask Policy**: `CREATE POLICY ... COLUMN MASK ... FOR TABLES WHEN ... MATCH COLUMNS ... ON COLUMN ... USING COLUMNS ...`
# MAGIC
# MAGIC > Requires Databricks Runtime 16.4+ or serverless compute.

# COMMAND ----------

# DBTITLE 1,Apply ABAC Policies
# Step 7: Apply ABAC (Attribute-Based Access Control) Policies
# ABAC policies use governed tags to dynamically enforce row filters
# and column masks across catalogs, schemas, or tables.

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

    # Build policy SQL
    sql = f"CREATE OR REPLACE POLICY {name}\n"
    sql += f"ON {scope_type} {scope}\n"
    if comment:
        sql += f"COMMENT '{comment}'\n"

    if policy_type == "ROW_FILTER":
        sql += f"ROW FILTER {udf}\n"
    elif policy_type == "COLUMN_MASK":
        sql += f"COLUMN MASK {udf}\n"

    # TO principals
    if to_principals:
        to_str = ", ".join([f"`{p}`" for p in to_principals])
        sql += f"TO {to_str}\n"

    # EXCEPT principals
    if except_principals:
        except_str = ", ".join([f"`{p}`" for p in except_principals])
        sql += f"EXCEPT {except_str}\n"

    sql += "FOR TABLES\n"

    # WHEN condition (table-level tag condition)
    if when_cond:
        sql += f"WHEN {when_cond}\n"

    # MATCH COLUMNS (column-level tag condition)
    if match_cols:
        sql += f"MATCH COLUMNS {match_cols}\n"

    # ON COLUMN (for column masks only)
    if policy_type == "COLUMN_MASK" and on_column:
        sql += f"ON COLUMN {on_column}\n"

    # USING COLUMNS (arguments passed to the UDF)
    if using_cols:
        using_str = ", ".join(using_cols)
        sql += f"USING COLUMNS ({using_str})\n"

    try:
        spark.sql(sql)
        print(f"  [OK] ABAC policy created: {name} ({policy_type} on {scope_type} {scope})")
    except Exception as e:
        print(f"  [FAIL] ABAC policy '{name}': {e}")

print("\n--- ABAC policies complete ---")

# COMMAND ----------

# DBTITLE 1,Step 8 — Verification
# MAGIC %md
# MAGIC ## Step 8: Verification
# MAGIC
# MAGIC Verify all applied security controls:
# MAGIC - Governed tags and their allowed values
# MAGIC - Tags on tables and columns
# MAGIC - UDF definitions
# MAGIC - RBAC grants
# MAGIC - Manual row filters and column masks
# MAGIC - ABAC policies (direct and effective)

# COMMAND ----------

# DBTITLE 1,Verify All Security Policies
# Step 8: Verification
# Verify all applied security policies and grants.

catalog_name = pipeline_config["catalog"]["name"]

print("=" * 70)
print("VERIFICATION: Data Security Pipeline")
print("=" * 70)

# 1. Verify governed tags
print("\n1. Governed Tags:")
for tag_def in pipeline_config["governed_tags"]:
    tag_key = tag_def["key"]
    try:
        result = spark.sql(f"DESCRIBE GOVERNED TAG `{tag_key}`")
        print(f"\n  Tag: {tag_key}")
        result.show(truncate=False)
    except Exception as e:
        print(f"  [FAIL] Could not describe tag {tag_key}: {e}")

# 2. Verify tags on tables
print("\n2. Table Tags:")
for table in pipeline_config.get("tables", []):
    full_name = table["full_name"]
    tbl_catalog = table["catalog"]
    tbl_schema = table["schema"]
    tbl_name = table["name"]
    print(f"\n  Tags on {full_name}:")
    try:
        spark.sql(f"""
            SELECT tag_name, tag_value
            FROM system.information_schema.table_tags
            WHERE catalog_name = '{tbl_catalog}'
              AND schema_name = '{tbl_schema}'
              AND table_name = '{tbl_name}'
        """).show(truncate=False)
    except Exception as e:
        print(f"    Error: {e}")

# 3. Verify UDFs
print("\n3. UDFs:")
for udf_def in pipeline_config["udfs"]:
    udf_name = udf_def["full_name"]
    try:
        spark.sql(f"DESCRIBE FUNCTION {udf_name}").show(truncate=False)
        print(f"  [OK] {udf_name}")
    except Exception as e:
        print(f"  [FAIL] {udf_name}: {e}")

# 4. Verify RBAC grants
print("\n4. RBAC Grants:")
for table in pipeline_config.get("tables", []):
    full_name = table["full_name"]
    print(f"\n  Grants on {full_name}:")
    try:
        spark.sql(f"SHOW GRANTS ON TABLE {full_name}").show(truncate=False)
    except Exception as e:
        print(f"    Error: {e}")

# 5. Verify manual row filters and column masks via SHOW CREATE TABLE
print("\n5. Manual Row Filters & Column Masks (via SHOW CREATE TABLE):")
for table in pipeline_config.get("tables", []):
    full_name = table["full_name"]
    print(f"\n  {full_name}:")
    try:
        result = spark.sql(f"SHOW CREATE TABLE {full_name}")
        create_stmt = result.collect()[0][0]
        # Show only the row filter / mask portions
        for line in create_stmt.split("\n"):
            if "ROW FILTER" in line.upper() or "MASK" in line.upper():
                print(f"    {line.strip()}")
    except Exception as e:
        print(f"    Error: {e}")

# 6. Verify ABAC policies
print("\n6. ABAC Policies:")
for policy in pipeline_config.get("abac_policies", []):
    name = policy["name"]
    scope_type = policy["scope_type"]
    scope = policy["scope"]
    try:
        print(f"\n  Policy: {name} (on {scope_type} {scope})")
        spark.sql(f"DESCRIBE POLICY {name} ON {scope_type} {scope}").show(truncate=False)
    except Exception as e:
        print(f"  [FAIL] Policy {name}: {e}")

# 7. Show effective policies on each table
print("\n7. Effective ABAC Policies on Tables:")
for table in pipeline_config.get("tables", []):
    full_name = table["full_name"]
    print(f"\n  Effective policies on {full_name}:")
    try:
        spark.sql(f"SHOW EFFECTIVE POLICIES ON TABLE {full_name}").show(truncate=False)
    except Exception as e:
        print(f"    Error: {e}")

print("\n" + "=" * 70)
print("VERIFICATION COMPLETE")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC All data security controls have been applied in chronological order:
# MAGIC
# MAGIC | Step | Security Layer | Method |
# MAGIC |------|---------------|--------|
# MAGIC | 1 | Governed Tags | `CREATE TAG` + `ALTER TAG ADD VALUE` |
# MAGIC | 2 | Tag Application | `ALTER TABLE ... SET TAGS` / `ALTER COLUMN ... SET TAGS` |
# MAGIC | 3 | UDFs | `CREATE OR REPLACE FUNCTION ... LANGUAGE PYTHON/SQL` |
# MAGIC | 4 | RBAC | `GRANT ... ON ... TO ...` |
# MAGIC | 5 | Row-Level Security | `ALTER TABLE ... SET ROW FILTER` |
# MAGIC | 6 | Column Masking | `ALTER TABLE ... ALTER COLUMN ... SET MASK` |
# MAGIC | 7 | ABAC Policies | `CREATE OR REPLACE POLICY ... ROW FILTER / COLUMN MASK` |
# MAGIC | 8 | Verification | `SHOW POLICIES`, `SHOW GRANTS`, `DESCRIBE`, `SHOW TAGS` |
# MAGIC
# MAGIC All configuration is maintained centrally in `config/pipeline_config.json`.\nTo modify security rules, update the JSON config and re-run this notebook.

# COMMAND ----------

