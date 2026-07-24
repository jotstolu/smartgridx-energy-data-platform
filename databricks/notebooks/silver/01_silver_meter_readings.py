# Databricks notebook source
# MAGIC %md
# MAGIC # SmartGridX - Silver Meter Readings
# MAGIC
# MAGIC This notebook transforms raw Bronze meter readings into clean Silver records.
# MAGIC
# MAGIC It also writes invalid records into a quarantine table.

# COMMAND ----------

import sys
import uuid
from datetime import datetime

# COMMAND ----------
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("catalog", "smartgridx_dev")
dbutils.widgets.text("run_id", "")

environment = dbutils.widgets.get("environment")
catalog = dbutils.widgets.get("catalog")
run_id_param = dbutils.widgets.get("run_id")

run_id  = run_id_param if run_id_param else str(uuid.uuid4())

bronze_schema = "bronze"
silver_schema = "silver"
audit_schema = "audit"

bronze_table = f"{catalog}.{bronze_schema}.meter_readings_raw"
clean_table_name = "meter_readings_clean"
qurantine_table_name = "meter_readings_qurantine"

clean_table = f"{catalog}.{silver_schema}.{clean_table_name}"
quarantine_table = f"{catalog}.{silver_schema}.{quarantine_table_name}"


# COMMAND ----------

# Adjust this path if your Databricks Git folder path is different.
sys.path.append("/Workspace/Users/olaoluwasolademi310@gmail.com/smartgridx-energy-data-platform/databricks/src")

from smartgridx.silver_utils import (
    create_silver_audit_table,
    write_silver_audit_record,
    standardise_string_columns,
    cast_meter_reading_columns,
    add_meter_reading_quality_flags,
    add_duplicate_flag,
    add_quarantine_reason,
    split_clean_and_quarantine,
    write_delta_table,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create schemas and audit table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{audit_schema}")


create_silver_audit_table(
    spark = spark,
    catalog  catalog,
    audit_schema = audit_schema,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Bronze table

# COMMAND ----------

started_at = datetime.utcnow()

try:
    bronze_df = spark.table(bronze_table)

except Exception as exc:
    raise RuntimeError(f"failed to read Bronze table {bronze_table}: {str(exc)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Standardise, cast and validate

# COMMAND ----------

try:
    transformed_df = standardise_string_columns(bronze_df)

    transformed_df = cast_meter_reading_columns(transformed_df)

    transformed_df = add_meter_reading_quality_flags(transformed_df)

    transformed_df = add_duplicate_flag(transformed_df)

    transformed_df = add_quarantine_reason(transformed_df)

    clean_df, quarantine_df = split_clean_and_quarantine(transformed_df)

    clean_records = clean_df.count()
    
    quarantined_records = quarantine_df.count()
    
    duplicate_records = transformed_df.filter("is_duplicate_reading = true").count()

except Exception as exc:
    raise RuntimeError(f"Failed during Silver transformation: {str(exc)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Silver tables

# COMMAND ----------

try:
    write_delta_table(
        df = clean_df,
        catalog = catalog,
        schema_name = silver_schema,
        table_name = clean_table_name,
        mode = "overwrite",
    )

    write_delta_table(
        df = qurantine_df,
        catalog = catalog,
        schema_name = silver_schema,
        table_name = quarantine_table_name,
        mode = "overwrite",
    )

    status = "SUCCESS"
    error_message = None

except Exception as exc:
    status = "FAILED"
    error_message = str(exc)
    raise

finally:
    audit_record = {
        "run_id": run_id,
        "source_table": bronze_table,
        "clean_table": clean_table,
        "quarantine_table": quarantine_table,
        "status": status,
        "bronze_records": bronze_records,
        "clean_records": clean_records if "clean_records" in locals() else 0,
        "quarantined_records": quarantined_records if "quarantined_records" in locals() else 0,
        "duplicate_records": duplicate_records if "duplicate_records" in locals() else 0,
        "error_message": error_message,
        "started_at_utc": started_at,
        "ended_at_utc": datetime.utcnow(),
        "environment": environment,
    }

     write_silver_audit_record(
        spark=spark,
        catalog=catalog,
        audit_schema=audit_schema,
        audit_record=audit_record,
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Display quality summary

# COMMAND ----------

display(
    quarantine_df.groupBy("quarantine_reason")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Display clean sample

# COMMAND ----------

display(
    spark.table(clean_table)
    .select(
        "reading_id",
        "meter_id",
        "customer_id",
        "reading_timestamp",
        "reading_date",
        "consumption_kwh",
        "voltage",
        "reading_source",
        "meter_status",
        "region",
        "firmware_version",
        "signal_strength_dbm",
        "meter_reading_quality_code",
        "silver_processed_at_utc",
    )
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Display audit record

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT *
        FROM {catalog}.{audit_schema}.silver_processing_audit
        WHERE run_id = '{run_id}'
        """
    )
)
