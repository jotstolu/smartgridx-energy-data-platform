# Databricks notebook source
# MAGIC %md
# MAGIC # SmartGridX - Bronze Ingestion
# MAGIC
# MAGIC This notebook loads raw source files into Bronze Delta tables.
# MAGIC
# MAGIC Bronze principles:
# MAGIC - Preserve raw source data
# MAGIC - Add ingestion metadata
# MAGIC - Support schema drift
# MAGIC - Write to Delta tables
# MAGIC - Record audit logs

# COMMAND ----------

import sys
import uuid
from datetime import datetime

from pyspark.sql import functions as F

# COMMAND ----------

# In Databricks Repos, this path may need to point to your repo root.
# We will adjust this when we run it in Databricks.
sys.path.append("/Workspace/Repos/smartgridx-energy-data-platform/databricks/src")

from smartgridx.bronze_utils import (
    load_yaml_config,
    create_database_if_not_exists,
    add_bronze_metadata,
    read_raw_csv,
    write_bronze_delta,
    get_table_count,
    create_audit_table,
    write_audit_record,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Runtime parameters

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("config_path", "/Workspace/Repos/smartgridx-energy-data-platform/databricks/jobs/bronze_ingestion_config.yml")
dbutils.widgets.text("run_id", "")

environment = dbutils.widgets.get("environment")
config_path = dbutils.widgets.get("config_path")
run_id_param = dbutils.widgets.get("run_id")

run_id = run_id_param if run_id_param else str(uuid.uuid4())

print(f"Environment: {environment}")
print(f"Config path: {config_path}")
print(f"Run ID: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load configuration

# COMMAND ----------

config = load_yaml_config(config_path)

catalog = config["lakehouse"]["catalog"]
bronze_schema = config["lakehouse"]["bronze_schema"]
silver_schema = config["lakehouse"]["silver_schema"]
gold_schema = config["lakehouse"]["gold_schema"]
audit_schema = config["lakehouse"]["audit_schema"]

raw_base_path = config["paths"]["raw_base_path"]

print(f"Catalog: {catalog}")
print(f"Bronze schema: {bronze_schema}")
print(f"Raw base path: {raw_base_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create schemas and audit table

# COMMAND ----------

create_database_if_not_exists(spark, catalog, bronze_schema)
create_database_if_not_exists(spark, catalog, silver_schema)
create_database_if_not_exists(spark, catalog, gold_schema)
create_database_if_not_exists(spark, catalog, audit_schema)

create_audit_table(spark, catalog, audit_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingest sources into Bronze

# COMMAND ----------

for source in config["sources"]:
    source_name = source["source_name"]
    source_path = source["source_path"]
    file_pattern = source["file_pattern"]
    target_table = source["target_table"]
    load_type = source["load_type"]
    multiline = source.get("multiline", False)

    started_at = datetime.utcnow()

    full_source_path = f"{raw_base_path}/{source_path}/{file_pattern}"

    print("=" * 80)
    print(f"Starting source: {source_name}")
    print(f"Source path: {full_source_path}")
    print(f"Target table: {catalog}.{bronze_schema}.{target_table}")
    print(f"Load type: {load_type}")

    try:
        raw_df = read_raw_csv(
            spark=spark,
            file_path=full_source_path,
            multiline=multiline,
        )

        bronze_df = add_bronze_metadata(
            df=raw_df,
            source_name=source_name,
            run_id=run_id,
            environment=environment,
        )

        records_loaded = bronze_df.count()

        write_bronze_delta(
            df=bronze_df,
            catalog=catalog,
            schema_name=bronze_schema,
            table_name=target_table,
            load_type=load_type,
        )

        audit_record = {
            "run_id": run_id,
            "source_name": source_name,
            "target_table": f"{catalog}.{bronze_schema}.{target_table}",
            "source_path": full_source_path,
            "file_pattern": file_pattern,
            "load_type": load_type,
            "status": "SUCCESS",
            "records_loaded": records_loaded,
            "error_message": None,
            "started_at_utc": started_at,
            "ended_at_utc": datetime.utcnow(),
            "environment": environment,
        }

        write_audit_record(
            spark=spark,
            catalog=catalog,
            audit_schema=audit_schema,
            audit_record=audit_record,
        )

        print(f"SUCCESS: {source_name}")
        print(f"Records loaded: {records_loaded:,}")

    except Exception as exc:
        error_message = str(exc)

        audit_record = {
            "run_id": run_id,
            "source_name": source_name,
            "target_table": f"{catalog}.{bronze_schema}.{target_table}",
            "source_path": full_source_path,
            "file_pattern": file_pattern,
            "load_type": load_type,
            "status": "FAILED",
            "records_loaded": 0,
            "error_message": error_message[:1000],
            "started_at_utc": started_at,
            "ended_at_utc": datetime.utcnow(),
            "environment": environment,
        }

        write_audit_record(
            spark=spark,
            catalog=catalog,
            audit_schema=audit_schema,
            audit_record=audit_record,
        )

        print(f"FAILED: {source_name}")
        print(error_message)

        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Show Bronze table counts

# COMMAND ----------

for source in config["sources"]:
    table_name = source["target_table"]
    row_count = get_table_count(
        spark=spark,
        catalog=catalog,
        schema_name=bronze_schema,
        table_name=table_name,
    )

    print(f"{catalog}.{bronze_schema}.{table_name}: {row_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Show audit results

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT *
        FROM {catalog}.{audit_schema}.bronze_ingestion_audit
        WHERE run_id = '{run_id}'
        ORDER BY started_at_utc
        """
    )
)