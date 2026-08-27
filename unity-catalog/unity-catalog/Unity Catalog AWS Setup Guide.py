# Databricks notebook source
# DBTITLE 1,Unity Catalog AWS Setup Overview
# MAGIC %md
# MAGIC # Unity Catalog AWS Setup Guide
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook contains detailed instructions for setting up Unity Catalog on AWS.
# MAGIC
# MAGIC **AWS Resources:**
# MAGIC - S3 Bucket: `my-data-world-bucket`
# MAGIC - IAM Role: `unity-catalog-metastore-role`
# MAGIC - IAM Policy: `unity-catalog-metastore-policy`
# MAGIC
# MAGIC **Databricks Account ID:** `414351767826`

# COMMAND ----------

# DBTITLE 1,Step 1: Create IAM Policy
# MAGIC %md
# MAGIC ## Step 1: Create the IAM Policy for S3 Access
# MAGIC
# MAGIC 1. Go to **AWS Console** → **IAM** → **Policies** → Click **Create Policy**
# MAGIC 2. Select **JSON** tab and paste this policy:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "Version": "2012-10-17",
# MAGIC   "Statement": [
# MAGIC     {
# MAGIC       "Sid": "UnityCatalogBucketAccess",
# MAGIC       "Effect": "Allow",
# MAGIC       "Action": [
# MAGIC         "s3:GetObject",
# MAGIC         "s3:PutObject",
# MAGIC         "s3:DeleteObject",
# MAGIC         "s3:ListBucket",
# MAGIC         "s3:GetBucketLocation",
# MAGIC         "s3:GetLifecycleConfiguration",
# MAGIC         "s3:PutLifecycleConfiguration"
# MAGIC       ],
# MAGIC       "Resource": [
# MAGIC         "arn:aws:s3:::my-data-world-bucket/*",
# MAGIC         "arn:aws:s3:::my-data-world-bucket"
# MAGIC       ]
# MAGIC     },
# MAGIC     {
# MAGIC       "Sid": "UnityCatalogKMSAccess",
# MAGIC       "Effect": "Allow",
# MAGIC       "Action": [
# MAGIC         "kms:Decrypt",
# MAGIC         "kms:Encrypt",
# MAGIC         "kms:GenerateDataKey"
# MAGIC       ],
# MAGIC       "Resource": [
# MAGIC         "arn:aws:kms:*:*:key/*"
# MAGIC       ],
# MAGIC       "Condition": {
# MAGIC         "StringLike": {
# MAGIC           "kms:ViaService": [
# MAGIC             "s3.*.amazonaws.com"
# MAGIC           ]
# MAGIC         }
# MAGIC       }
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC 3. Click **Next: Tags** (add tags if needed)
# MAGIC 4. Click **Next: Review**
# MAGIC 5. **Name**: `unity-catalog-metastore-policy`
# MAGIC 6. **Description**: `Policy for Unity Catalog metastore access to S3`
# MAGIC 7. Click **Create Policy**

# COMMAND ----------

# DBTITLE 1,Step 2: Create IAM Role
# MAGIC %md
# MAGIC ## Step 2: Create the IAM Role
# MAGIC
# MAGIC 1. Go to **IAM** → **Roles** → Click **Create Role**
# MAGIC 2. Select **Trusted Entity Type**: **AWS Account**
# MAGIC 3. Select **Another AWS Account**
# MAGIC 4. **Account ID**: Enter `414351767826` (Databricks AWS Account ID)
# MAGIC 5. Check **Require external ID**
# MAGIC 6. **External ID**: Use a placeholder for now like `temp-external-id` (you'll update this later after creating the credential in Databricks)
# MAGIC 7. Click **Next**

# COMMAND ----------

# DBTITLE 1,Step 3: Attach Policy
# MAGIC %md
# MAGIC ## Step 3: Attach the Policy to the Role
# MAGIC
# MAGIC 1. In the **Add permissions** screen, search for `unity-catalog-metastore-policy`
# MAGIC 2. Check the box next to the policy you created in Step 1
# MAGIC 3. Click **Next**

# COMMAND ----------

# DBTITLE 1,Step 4: Name and Create Role
# MAGIC %md
# MAGIC ## Step 4: Name and Create the Role
# MAGIC
# MAGIC 1. **Role name**: `unity-catalog-metastore-role`
# MAGIC 2. **Description**: `IAM role for Databricks Unity Catalog metastore`
# MAGIC 3. Review the settings
# MAGIC 4. Click **Create Role**

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 4.1: Create Storage Credential
# MAGIC
# MAGIC # List existing storage credentials, and create storage credential 'asw_metastore' if it does not exist

# COMMAND ----------

# DBTITLE 1,Create Storage Credential (Using SDK)

# Create storage credential using Databricks SDK
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import AwsIamRoleRequest

w = WorkspaceClient()
acount_id='311883574873'
role='unity-catalog-metastore-role'
# Your IAM role ARN - REPLACE with your actual AWS account ID
role_arn = f"arn:aws:iam::{acount_id}:role/{role}"

try:
    # Create storage credential
    cred = w.storage_credentials.create(
        name="aws_unity_metastore",
        aws_iam_role=AwsIamRoleRequest(role_arn=role_arn),
        comment="Storage credential for Unity Catalog metastore on S3"
    )
    print(f"✅ Storage credential '{cred.name}' created successfully!")
    print(f"Created at: {cred.created_at}")
    print(f"Created by: {cred.created_by}")
except Exception as e:
    if "already exists" in str(e):
        print("⚠️  Storage credential already exists")
    else:
        print(f"❌ Error: {e}")

# List all storage credentials
print("\n" + "="*60)
print("All Storage Credentials:")
print("="*60)
for sc in w.storage_credentials.list():
    print(f"- {sc.name}")

# COMMAND ----------

# DBTITLE 1,Get External ID Using Databricks SDK
# Get the external ID for the storage credential
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Retrieve the storage credential
cred = w.storage_credentials.get(name="aws_unity_metastore")

print("\n" + "="*70)
print("STORAGE CREDENTIAL DETAILS")
print("="*70)
print(f"Name: {cred.name}")
print(f"Owner: {cred.owner}")
print(f"Created: {cred.created_at}")

if cred.aws_iam_role:
    print(f"\nRole ARN: {cred.aws_iam_role.role_arn}")
    print("\n" + "="*70)
    print("⭐ EXTERNAL ID (Copy this to AWS IAM Trust Policy):")
    print("="*70)
    print(f"\n{cred.aws_iam_role.external_id}\n")
    print("="*70)
    print("\n✅ Use this External ID in Step 5 to update your IAM role trust policy")
else:
    print("\n❌ No AWS IAM role configured for this credential")

# COMMAND ----------

# DBTITLE 1,Step 5: Update Trust Relationship
# MAGIC %md
# MAGIC ## Step 5: Update the Trust Relationship (After Getting External ID from Databricks)
# MAGIC
# MAGIC After creating the metastore credential in Databricks, you'll receive an External ID. Update the trust policy:
# MAGIC
# MAGIC 1. Go to **IAM** → **Roles** → Find `unity-catalog-metastore-role`
# MAGIC 2. Go to **Trust relationships** tab
# MAGIC 3. Click **Edit trust policy**
# MAGIC 4. Replace with this (update the External ID):
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "Version": "2012-10-17",
# MAGIC   "Statement": [
# MAGIC     {
# MAGIC       "Effect": "Allow",
# MAGIC       "Principal": {
# MAGIC         "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
# MAGIC       },
# MAGIC       "Action": "sts:AssumeRole",
# MAGIC       "Condition": {
# MAGIC         "StringEquals": {
# MAGIC           "sts:ExternalId": "YOUR-EXTERNAL-ID-FROM-DATABRICKS"
# MAGIC         }
# MAGIC       }
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC 5. Click **Update policy**

# COMMAND ----------

# DBTITLE 1,Step 6: Copy Role ARN
# MAGIC %md
# MAGIC ## Step 6: Copy the Role ARN
# MAGIC
# MAGIC 1. In the role summary page, copy the **ARN** (looks like: `arn:aws:iam::YOUR-ACCOUNT-ID:role/unity-catalog-metastore-role`)
# MAGIC 2. You'll need this ARN when creating the Unity Catalog metastore in Databricks
# MAGIC
# MAGIC **Note:** Save this ARN - you'll use it in the next steps when configuring Databricks Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Summary and Next Steps
# MAGIC %md
# MAGIC ## Summary of What You Created
# MAGIC
# MAGIC * **IAM Policy**: `unity-catalog-metastore-policy` - Grants permissions to access your S3 bucket
# MAGIC * **IAM Role**: `unity-catalog-metastore-role` - Allows Databricks to assume this role to access your bucket
# MAGIC * **Trust Relationship**: Configured to trust Databricks AWS account with external ID validation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Important Notes
# MAGIC
# MAGIC ⚠️ **Security Best Practices:**
# MAGIC - Keep your External ID secure
# MAGIC - Regularly review IAM role permissions
# MAGIC - Enable CloudTrail logging for audit purposes
# MAGIC - Consider using AWS KMS for encryption at rest
# MAGIC
# MAGIC ✅ **Validation Checklist:**
# MAGIC - [ ] S3 bucket `my-data-world-bucket` created
# MAGIC - [ ] IAM policy `unity-catalog-metastore-policy` created
# MAGIC - [ ] IAM role `unity-catalog-metastore-role` created
# MAGIC - [ ] Policy attached to role
# MAGIC - [ ] Role ARN copied
# MAGIC - [ ] Ready to configure in Databricks

# COMMAND ----------

# DBTITLE 1,Next Steps Overview
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # Part 2: Create Metastore and Attach Workspace
# MAGIC
# MAGIC Now that the AWS infrastructure is ready, we'll:
# MAGIC 1. Create a Unity Catalog metastore
# MAGIC 2. Assign this workspace to the metastore
# MAGIC 3. Verify the setup

# COMMAND ----------

# DBTITLE 1,Step 7: Create Unity Catalog Metastore
# MAGIC %md
# MAGIC ## Step 7: Create Unity Catalog Metastore
# MAGIC
# MAGIC ### Option A: Using Databricks Account Console
# MAGIC
# MAGIC **Step-by-Step Navigation:**
# MAGIC
# MAGIC 1. Go to **Databricks Account Console**: https://accounts.cloud.databricks.com/
# MAGIC 2. **Important**: You'll land on a page showing workspaces. Look for the left sidebar navigation
# MAGIC 3. In the left sidebar, scroll down and look for **Catalog** section (might be under "Data" or directly visible)
# MAGIC 4. Click on **Catalog** or **Data** 
# MAGIC 5. Then click **Metastores**
# MAGIC 6. Click **Create Metastore** button
# MAGIC
# MAGIC **If you don't see Catalog/Data/Metastores option:**
# MAGIC - You might not have **Account Admin** permissions
# MAGIC - Contact your Databricks account administrator
# MAGIC - Or use **Option B** below if you have workspace admin permissions
# MAGIC
# MAGIC **When creating the metastore, fill in:**
# MAGIC - **Name**: `my-unity-metastore` (or your preferred name)
# MAGIC - **Region**: Select the AWS region matching your S3 bucket (e.g., `us-east-1`)
# MAGIC - **S3 Bucket Path**: `s3://my-data-world-bucket/metastore`
# MAGIC - **Storage Credential**: Select `aws_metastore` (the credential we created earlier)
# MAGIC - Click **Create**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Option B: Using Your Workspace (If you have workspace admin access)
# MAGIC
# MAGIC If you have workspace admin permissions but not account admin:
# MAGIC
# MAGIC 1. Stay in your **workspace** (not account console)
# MAGIC 2. Go to **Catalog** icon in the left sidebar (database icon)
# MAGIC 3. If Unity Catalog is not enabled, you'll see an option to **Set up Unity Catalog** or **Enable Unity Catalog**
# MAGIC 4. Follow the wizard to create a metastore
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Option C: Using Databricks SDK (Programmatic approach)
# MAGIC
# MAGIC See the next cell for Python code to create the metastore programmatically.

# COMMAND ----------

# DBTITLE 1,Create Metastore Using SDK (Optional)
# Check if you can access account-level APIs
from databricks.sdk import AccountClient, WorkspaceClient

print("=" * 70)
print("METASTORE CREATION - ACCOUNT ADMIN REQUIRED")
print("=" * 70)

print("\n⚠️  Important: Creating a metastore requires Account Admin permissions.")
print("\nChecking your access level...\n")

try:
    a = AccountClient()
    metastores = list(a.metastores.list())
    
    print("✅ You have Account Admin access!")
    print(f"\nFound {len(metastores)} existing metastore(s):")
    
    if metastores:
        for m in metastores:
            print(f"  • {m.name}")
            print(f"    Region: {m.region}")
            print(f"    Storage: {m.storage_root}")
            print()
        
        print("💡 You can assign your workspace to an existing metastore!")
        print("   See Step 8 for instructions.\n")
    else:
        print("\nNo metastores exist yet.")
        print("💡 You can create one using the Account Console:")
        print("   https://accounts.cloud.databricks.com/")
        print("   Navigate to: Catalog → Metastores → Create Metastore\n")
        
except Exception as e:
    print("❌ You do NOT have Account Admin access")
    print(f"Error: {e}\n")
    print("=" * 70)
    print("WHAT TO DO NEXT")
    print("=" * 70)
    print("\nOption 1: Ask your Account Admin to create the metastore")
    print("  • Run Cell 17 below to get the information they need")
    print("\nOption 2: Request Account Admin permissions")
    print("  • See Cell 18 for instructions\n")

# COMMAND ----------

# DBTITLE 1,Troubleshooting: Check Your Permissions
# MAGIC %md
# MAGIC ## Troubleshooting: Check Your Access Level
# MAGIC
# MAGIC Run the next cell to check what level of access you have.

# COMMAND ----------

# DBTITLE 1,Check Current User Permissions
# Check your current permissions and metastore status
from databricks.sdk import WorkspaceClient, AccountClient

w = WorkspaceClient()

print("=" * 70)
print("CHECKING YOUR ACCESS LEVEL")
print("=" * 70)

# Check workspace-level access
try:
    current_user = w.current_user.me()
    print(f"\n✅ Workspace User: {current_user.user_name}")
    print(f"Active: {current_user.active}")
    
    # Check if user is workspace admin
    is_admin = any(group.display == 'admins' for group in (current_user.groups or []))
    if is_admin:
        print("\n🔑 You have WORKSPACE ADMIN permissions")
    else:
        print("\n⚠️  You are NOT a workspace admin")
except Exception as e:
    print(f"❌ Error checking user: {e}")

# Check if Unity Catalog is already enabled
print("\n" + "=" * 70)
print("UNITY CATALOG STATUS")
print("=" * 70)

try:
    metastore = w.metastores.current()
    print("\n✅ Unity Catalog is ALREADY ENABLED!")
    print(f"Metastore Name: {metastore.name}")
    print(f"Metastore ID: {metastore.metastore_id}")
    print(f"Storage Root: {metastore.storage_root}")
    print("\n💡 You can skip metastore creation and go directly to Step 10!")
except Exception as e:
    print("\n⚠️  Unity Catalog is NOT enabled yet")
    print("You need to create and assign a metastore.")

# Check account-level access
print("\n" + "=" * 70)
print("ACCOUNT-LEVEL ACCESS")
print("=" * 70)

try:
    a = AccountClient()
    metastores = list(a.metastores.list())
    print(f"\n✅ You have ACCOUNT ADMIN access")
    print(f"Found {len(metastores)} metastore(s) in your account")
    if metastores:
        print("\nExisting Metastores:")
        for m in metastores:
            print(f"  - {m.name} (Region: {m.region})")
except Exception as e:
    print("\n⚠️  You do NOT have account admin access")
    print("Error:", str(e))
    print("\n💡 You'll need to either:")
    print("   1. Ask your account admin to create the metastore, OR")
    print("   2. Get account admin permissions")

# COMMAND ----------

# DBTITLE 1,Get Information for Your Account Admin
# Get information to share with your Account Admin
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

workspace_id = w.get_workspace_id()
current_user = w.current_user.me()

print("=" * 70)
print("INFORMATION TO SHARE WITH YOUR ACCOUNT ADMIN")
print("=" * 70)

print("\n📋 Copy this information and send to your Account Admin:\n")
print("-" * 70)
print("\nHi [Account Admin Name],\n")
print("I need help setting up Unity Catalog for our workspace.\n")
print("Could you please create a Unity Catalog metastore with these details:\n")
print(f"  • Metastore Name: my-unity-metastore")
print(f"  • Region: us-east-1 (or the region of our S3 bucket)")
print(f"  • S3 Bucket Path: s3://my-data-world-bucket/metastore")
print(f"  • Storage Credential: aws_metastore")
print(f"\nThen assign this workspace to the metastore:")
print(f"  • Workspace ID: {workspace_id}")
print(f"  • Requested by: {current_user.user_name}")
print("\nThank you!")
print("-" * 70)

# COMMAND ----------

# DBTITLE 1,Alternative: Request Account Admin Access
# MAGIC %md
# MAGIC ### Option 2: Request Account Admin Permissions
# MAGIC
# MAGIC If you need to manage Unity Catalog yourself, ask your current Account Admin to grant you Account Admin permissions:
# MAGIC
# MAGIC **Steps for the Account Admin:**
# MAGIC 1. Go to https://accounts.cloud.databricks.com/
# MAGIC 2. Click on **User management** in the left sidebar
# MAGIC 3. Find your user (`sskale2003@gmail.com`)
# MAGIC 4. Click on your user
# MAGIC 5. Under **Roles**, add the **Account admin** role
# MAGIC 6. Save changes
# MAGIC
# MAGIC Once you have Account Admin access, you'll be able to:
# MAGIC - See the **Catalog** → **Metastores** option in the Account Console
# MAGIC - Create and manage metastores
# MAGIC - Assign workspaces to metastores
# MAGIC - Manage storage credentials at the account level
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Option 3: Check if Your Organization Has an Existing Metastore
# MAGIC
# MAGIC Before requesting creation, check if there's already a metastore you can use. Ask your Account Admin to:
# MAGIC 1. Check if a metastore already exists in your account
# MAGIC 2. If yes, simply assign your workspace to the existing metastore
# MAGIC 3. This is faster and follows best practices (one metastore per region)

# COMMAND ----------

# DBTITLE 1,Step 8: Assign Workspace to Metastore
# MAGIC %md
# MAGIC ## Step 8: Assign Workspace to Metastore
# MAGIC
# MAGIC After creating the metastore, you need to assign your workspace to it.
# MAGIC
# MAGIC ### Option A: Using Databricks Account Console
# MAGIC
# MAGIC 1. In **Databricks Account Console**, go to **Data** → **Metastores**
# MAGIC 2. Click on your newly created metastore (`my-unity-metastore`)
# MAGIC 3. Go to the **Workspaces** tab
# MAGIC 4. Click **Assign to workspaces**
# MAGIC 5. Select your workspace from the list
# MAGIC 6. Click **Assign**
# MAGIC
# MAGIC ### Option B: Using Databricks SDK
# MAGIC
# MAGIC See the next cell for Python code.

# COMMAND ----------

# DBTITLE 1,Assign Workspace to Metastore (Optional)
# Assign workspace to metastore using Databricks SDK
from databricks.sdk import AccountClient, WorkspaceClient

try:
    # Account client for metastore operations
    a = AccountClient()
    
    # Workspace client to get current workspace info
    w = WorkspaceClient()
    workspace_id = w.get_workspace_id()
    
    print(f"Current Workspace ID: {workspace_id}")
    
    # List available metastores
    print("\nAvailable Metastores:")
    metastores = list(a.metastores.list())
    for m in metastores:
        print(f"  - {m.name} (ID: {m.metastore_id})")
    
    # Assign workspace to metastore
    # Replace 'my-unity-metastore' with your metastore ID if needed
    metastore_id = None
    for m in metastores:
        if m.name == "my-unity-metastore":
            metastore_id = m.metastore_id
            break
    
    if metastore_id:
        a.metastores.assign(
            workspace_id=workspace_id,
            metastore_id=metastore_id,
            default_catalog_name="main"  # Default catalog for the workspace
        )
        print(f"\n✅ Workspace {workspace_id} assigned to metastore '{metastore_id}'")
        print("Default catalog: main")
    else:
        print("\n⚠️  Metastore 'my-unity-metastore' not found. Use Account Console or adjust the name.")
        
except Exception as e:
    print(f"❌ Error: {e}")
    print("\nNote: This requires account-level permissions. Use the Account Console if needed.")

# COMMAND ----------

# DBTITLE 1,Step 9: Verify Unity Catalog Setup
# MAGIC %md
# MAGIC ## Step 9: Verify Unity Catalog Setup
# MAGIC
# MAGIC Once the workspace is assigned to the metastore, verify the setup by running these checks.

# COMMAND ----------

# DBTITLE 1,Verify Unity Catalog is Enabled
# Verify Unity Catalog is enabled and accessible
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

print("=" * 70)
print("UNITY CATALOG VERIFICATION")
print("=" * 70)

try:
    # Get current metastore
    metastore = w.metastores.current()
    print("\n✅ Unity Catalog is enabled!")
    print(f"\nMetastore Name: {metastore.name}")
    print(f"Metastore ID: {metastore.metastore_id}")
    print(f"Region: {metastore.region}")
    print(f"Storage Root: {metastore.storage_root}")
    print(f"Owner: {metastore.owner}")
    print(f"Default Catalog: {metastore.default_catalog_name}")
    
    # List catalogs
    print("\n" + "=" * 70)
    print("Available Catalogs:")
    print("=" * 70)
    for catalog in w.catalogs.list():
        print(f"  - {catalog.name}")
    
    print("\n✅ Unity Catalog setup is complete!")
    
except Exception as e:
    print(f"\n❌ Unity Catalog not enabled: {e}")
    print("\nPlease complete the metastore assignment steps.")

# COMMAND ----------

# DBTITLE 1,Step 10: Create Your First Catalog and Schema
# MAGIC %md
# MAGIC ## Step 10: Create Your First Catalog and Schema
# MAGIC
# MAGIC Now that Unity Catalog is set up, let's create a catalog and schema to organize your data.

# COMMAND ----------

# DBTITLE 1,Create Catalog and Schema
# Create a new catalog and schema

try:
    # Create catalog
    spark.sql("""
    CREATE CATALOG IF NOT EXISTS my_catalog
    COMMENT 'My first Unity Catalog catalog'
    """)
    print("✅ Catalog 'my_catalog' created")
    
    # Create schema
    spark.sql("""
    CREATE SCHEMA IF NOT EXISTS my_catalog.my_schema
    COMMENT 'My first schema'
    """)
    print("✅ Schema 'my_catalog.my_schema' created")
    
    # Set as default for this session
    spark.sql("USE CATALOG my_catalog")
    spark.sql("USE SCHEMA my_schema")
    print("\n✅ Default catalog and schema set")
    
    # Verify
    current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    current_schema = spark.sql("SELECT current_schema()").collect()[0][0]
    print(f"\nCurrent catalog: {current_catalog}")
    print(f"Current schema: {current_schema}")
    
    print("\n" + "=" * 70)
    print("🎉 You can now create tables in my_catalog.my_schema!")
    print("=" * 70)
    
except Exception as e:
    print(f"❌ Error: {e}")

# COMMAND ----------

# DBTITLE 1,Final Summary and Next Steps
# MAGIC %md
# MAGIC ## 🎉 Unity Catalog Setup Complete!
# MAGIC
# MAGIC ### What You've Accomplished:
# MAGIC
# MAGIC 1. ✅ Created AWS S3 bucket: `my-data-world-bucket`
# MAGIC 2. ✅ Created IAM policy: `unity-catalog-metastore-policy`
# MAGIC 3. ✅ Created IAM role: `unity-catalog-metastore-role`
# MAGIC 4. ✅ Created Databricks storage credential
# MAGIC 5. ✅ Created Unity Catalog metastore
# MAGIC 6. ✅ Assigned workspace to metastore
# MAGIC 7. ✅ Created first catalog and schema
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Next Steps - Start Using Unity Catalog:
# MAGIC
# MAGIC **Create Tables:**
# MAGIC ```sql
# MAGIC CREATE TABLE my_catalog.my_schema.customers (
# MAGIC   customer_id INT,
# MAGIC   name STRING,
# MAGIC   email STRING
# MAGIC ) USING DELTA;
# MAGIC ```
# MAGIC
# MAGIC **Load Data:**
# MAGIC ```python
# MAGIC df = spark.read.csv("s3://your-data-bucket/data.csv", header=True)
# MAGIC df.write.saveAsTable("my_catalog.my_schema.customers")
# MAGIC ```
# MAGIC
# MAGIC **Query Tables:**
# MAGIC ```sql
# MAGIC SELECT * FROM my_catalog.my_schema.customers LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC **Manage Permissions:**
# MAGIC ```sql
# MAGIC GRANT SELECT ON TABLE my_catalog.my_schema.customers TO `user@example.com`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Useful Resources:
# MAGIC
# MAGIC * [Unity Catalog Documentation](https://docs.databricks.com/unity-catalog/index.html)
# MAGIC * [Unity Catalog Best Practices](https://docs.databricks.com/unity-catalog/best-practices.html)
# MAGIC * [Data Governance with Unity Catalog](https://docs.databricks.com/data-governance/index.html)