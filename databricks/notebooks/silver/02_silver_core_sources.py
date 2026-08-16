# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SmartGridX - Silver Core Sources
# MAGIC
# MAGIC this notebook cleans and validates non meter reading Bronze sources:
# MAGIC - customers
# MAGIC - tariffs
# MAGIC - meters
# MAGIC - weather
# MAGIC - outage events
# MAGIC - billing events

# COMMAND ----------

import sys
import uuid
from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

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

# COMMAND ----------

sys.path.append("/Workspace/Repos/olaoluwasolademi310@gmail.com/smartgridx-energy-data-platform/databricks/src")

from smartgridx.silver_utils import (
    create_silver_audit_table,
    write_silver_audit_record,
    standardise_string_columns,
    write_delta_table,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create schemas and audit table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{audit_schema}")

create_silver_audit_table(
    spark=spark,
    catalog=catalog,
    audit_schema=audit_schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generic helper functions

# COMMAND ----------

def add_duplicate_flag(df: DataFrame, key_col: str, flag_col: str)  -> DataFrame:
    """
    Flag duplicate rows based on a business key.

    """
    window_spec = Window.partitionBy(key_col).orderBy(F.col("_ingested_at_utc").desc_nulls_last())

    return(
        df.withColumn("_duplicate_rank", F.row_number().over(window_spec))
        .withColumn(
            flag_col,
            F.col(key_col).isNotNull() & (F.col("_duplicate_rank") > 1)
        )
    )

def add_quarantine_reason(df:DataFrame, reason_map: dict) -> DataFrame:
    """
    Build a readable quarantine reason from boolean validation flags.

    """
    reason_expressions = [
        F.when(F.col(flag_col), F.lit(reason_text))
        for flag_col, reason_text in reason_map.items()
    ]

    return df.withColumn("quarantine_reason", F.concat_ws("; ", *reason_expressions))


def split_clean_and_quarantine_generic(df: DataFrame, reason_map: dict,) -> tuple[DataFrame, DataFrame]:
    """
    split a validated dataframe into clean and quarantine dataframes
    """

    issue_expression = reduce(
        lambda left, right: left | right,
        [F.coalesce(F.col(flag_col), F.lit(False)) for flag_col in reason_map.keys()]
    )

    quarantine_df = (
        df.filter(issue_expression).withColumn("quarantined_at_utc", F.current_timestamp())
    )

    clean_df = (
        df.filter(~issue_expression).drop("_duplicate_rank", "quarantine_reason")
        .withColumn("silver_processed_at_utc", F.current_timestamp())
    )

    return clean_df, quarantine_df


def add_reference_check(df: DataFrame, reference_df: DataFrame, join_col: str, flag_col: str) -> DataFrame:
    """
    flag records where a foreign key does not exist in a reference table.

    """
    ref_col_name = f"_ref_{join_col}"
    ref_df = (
        reference_df.select(F.col(join_col).alias(ref_col_name))
        .dropDuplicates()
        .withColumn("_reference_exists", F.lit(True))
    )

    joined_df = df.join(F.broadcast(ref_df),
    df[join_col] == ref_df[ref_col_name], "left")

    return (
        joined_df.withColumn(
            flag_col, F.col(join_col).isNotNull() & F.col("_reference_exists").isNull(),
        ).drop(ref_col_name, "_reference_exists")
    )


def process_silver_table(
    source_name: str,
    bronze_table_name: str,
    clean_table_name: str,
    quarantine_table_name: str,
    transform_function,
) -> None:
    """
    Generic processor for one silver source

    """
    bronze_table = f"{catalog}.{bronze_schema}.{bronze_table_name}"
    clean_table = f"{catalog}.{silver_schema}.{clean_table_name}"
    quarantine_table = f"{catalog}.{silver_schema}.{quarantine_table_name}"

    started_at = datetime.utcnow()

    print("*=*" * 100)
    print(f"processing source: {source_name}")
    print(f"Bronze table: {bronze_table}")
    print(f"clean table: {clean_table}")
    print(f"Quarantine table: {quarantine_table}")

    status = "SUCCESS"
    error_message = ""
    bronze_records = 0
    clean_records = 0
    quarantined_records = 0
    duplicate_records = 0


    try:
        bronze_df = spark.table(bronze_table)
        bronze_records = bronze_df.count()

        transformed_df, reason_map, duplicate_flag_col = transform_function(bronze_df)

        transformed_df = add_quarantine_reason(transformed_df, reason_map)

        clean_df, quarantined_df = split_clean_and_quarantine_generic(
            transformed_df,
            reason_map
        )

        clean_records = clean_df.count()

        quarantined_records = quarantined_df.count()

        if duplicate_flag_col:
            duplicate_records = transformed_df.filter(F.col(duplicate_flag_col)).count()
        
        write_delta_table(
            df= clean_df,
            catalog = catalog,
            schema_name = silver_schema,
            table_name = clean_table_name,
            mode = "overwrite"
        )


        write_delta_table(
            df=quarantined_df,
            catalog=catalog,
            schema_name=silver_schema,
            table_name=quarantine_table_name,
            mode="overwrite",
        )

        print(f"SUCCESS: {source_name}")
        print(f"Bronze records: {bronze_records:,}")
        print(f"Clean records: {clean_records:,}")
        print(f"Quarantined records: {quarantined_records:,}")
        print(f"Duplicate records: {duplicate_records:,}")

    except Exception as exc:
        status = "FAILED"
        error_message = str(exc)
        print(f"FAILED: {source_name}")
        print(error_message)
        raise

    finally:
        audit_record = {
            "run_id": run_id,
            "source_table": bronze_table,
            "clean_table": clean_table,
            "quarantine_table": quarantine_table,
            "status": status,
            "bronze_records": bronze_records,
            "clean_records": clean_records,
            "quarantined_records": quarantined_records,
            "duplicate_records": duplicate_records,
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
# MAGIC ## 3. Source-specific transformation rules

# COMMAND ----------

def transform_customers(bronze_df: DataFrame):
    valid_statuses = ["Active", "Suspended", "Closed"]

    df = standardise_string_columns(bronze_df)

    df = (
        df.withColumn("registration_date", F.to_date("registration_date"))
        .withColumn("generated_at_utc", F.to_timestamp("generated_at_utc"))
        .withColumn("_ingested_at_utc", F.to_timestamp("_ingested_at_utc"))
    )

    df = add_duplicate_flag(df, "customer_id", "is_duplicate_customer_id")

    df = (
        df.withColumn("is_missing_customer_id", F.col("customer_id").isNull() | (F.col("customer_id") == ""))
        .withColumn("is_missing_customer_name", F.col("customer_name").isNull() | (F.col("customer_name") == ""))
        .withColumn("is_invalid_email", ~F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"))
        .withColumn("is_missing_region", F.col("region").isNull() | (F.col("region") == ""))
        .withColumn("is_invalid_account_status", ~F.col("account_status").isin(*valid_statuses))
        .withColumn("is_missing_registration_date", F.col("registration_date").isNull())
    )

    reason_map = {
        "is_missing_customer_id": "Missing customer_id",
        "is_missing_customer_name": "Missing customer_name",
        "is_invalid_email": "Invalid email format",
        "is_missing_region": "Missing region",
        "is_invalid_account_status": "Invalid account_status",
        "is_missing_registration_date": "Missing or invalid registration_date",
        "is_duplicate_customer_id": "Duplicate customer_id",
    }

    return df, reason_map, "is_duplicate_customer_id"


def transform_tariffs(bronze_df: DataFrame):
    df = standardise_string_columns(bronze_df)

    df = (
        df.withColumn("standing_charge_pence_per_day", F.col("standing_charge_pence_per_day").cast("double"))
        .withColumn("unit_rate_pence_per_kwh", F.col("unit_rate_pence_per_kwh").cast("double"))
        .withColumn("green_energy_flag", F.col("green_energy_flag").cast("boolean"))
        .withColumn("effective_start_date", F.to_date("effective_start_date"))
        .withColumn("effective_end_date", F.to_date("effective_end_date"))
        .withColumn("generated_at_utc", F.to_timestamp("generated_at_utc"))
        .withColumn("_ingested_at_utc", F.to_timestamp("_ingested_at_utc"))
    )

    df = add_duplicate_flag(df, "tariff_id", "is_duplicate_tariff_id")

    df = (
        df.withColumn("is_missing_tariff_id", F.col("tariff_id").isNull() | (F.col("tariff_id") == ""))
        .withColumn("is_missing_tariff_name", F.col("tariff_name").isNull() | (F.col("tariff_name") == ""))
        .withColumn("is_invalid_standing_charge", F.col("standing_charge_pence_per_day").isNull() | (F.col("standing_charge_pence_per_day") < 0))
        .withColumn("is_invalid_unit_rate", F.col("unit_rate_pence_per_kwh").isNull() | (F.col("unit_rate_pence_per_kwh") <= 0))
        .withColumn("is_missing_effective_start_date", F.col("effective_start_date").isNull())
        .withColumn(
            "is_invalid_effective_date_range",
            F.col("effective_end_date").isNotNull() & (F.col("effective_end_date") < F.col("effective_start_date")),
        )
    )

    reason_map = {
        "is_missing_tariff_id": "Missing tariff_id",
        "is_missing_tariff_name": "Missing tariff_name",
        "is_invalid_standing_charge": "Invalid standing charge",
        "is_invalid_unit_rate": "Invalid unit rate",
        "is_missing_effective_start_date": "Missing effective_start_date",
        "is_invalid_effective_date_range": "effective_end_date earlier than effective_start_date",
        "is_duplicate_tariff_id": "Duplicate tariff_id",
    }

    return df, reason_map, "is_duplicate_tariff_id"


def transform_meters(bronze_df: DataFrame):
    valid_meter_types = ["SMETS1", "SMETS2"]
    valid_meter_statuses = ["Active", "Faulty", "Disconnected"]

    customers_clean_df = spark.table(f"{catalog}.{silver_schema}.customers_clean")
    tariffs_clean_df = spark.table(f"{catalog}.{silver_schema}.tariffs_clean")

    df = standardise_string_columns(bronze_df)

    df = (
        df.withColumn("installation_date", F.to_date("installation_date"))
        .withColumn("generated_at_utc", F.to_timestamp("generated_at_utc"))
        .withColumn("_ingested_at_utc", F.to_timestamp("_ingested_at_utc"))
    )

    df = add_duplicate_flag(df, "meter_id", "is_duplicate_meter_id")

    df = add_reference_check(
        df=df,
        reference_df=customers_clean_df,
        join_col="customer_id",
        flag_col="is_unknown_customer_id",
    )

    df = add_reference_check(
        df=df,
        reference_df=tariffs_clean_df,
        join_col="tariff_id",
        flag_col="is_unknown_tariff_id",
    )

    df = (
        df.withColumn("is_missing_meter_id", F.col("meter_id").isNull() | (F.col("meter_id") == ""))
        .withColumn("is_missing_customer_id", F.col("customer_id").isNull() | (F.col("customer_id") == ""))
        .withColumn("is_missing_tariff_id", F.col("tariff_id").isNull() | (F.col("tariff_id") == ""))
        .withColumn("is_invalid_meter_type", ~F.col("meter_type").isin(*valid_meter_types))
        .withColumn("is_invalid_meter_status", ~F.col("meter_status").isin(*valid_meter_statuses))
        .withColumn("is_missing_installation_date", F.col("installation_date").isNull())
    )

    reason_map = {
        "is_missing_meter_id": "Missing meter_id",
        "is_missing_customer_id": "Missing customer_id",
        "is_missing_tariff_id": "Missing tariff_id",
        "is_unknown_customer_id": "customer_id does not exist in customers_clean",
        "is_unknown_tariff_id": "tariff_id does not exist in tariffs_clean",
        "is_invalid_meter_type": "Invalid meter_type",
        "is_invalid_meter_status": "Invalid meter_status",
        "is_missing_installation_date": "Missing or invalid installation_date",
        "is_duplicate_meter_id": "Duplicate meter_id",
    }

    return df, reason_map, "is_duplicate_meter_id"


def transform_weather(bronze_df: DataFrame):
    valid_conditions = ["Clear", "Cloudy", "Rain", "Heavy Rain", "Snow", "Fog", "Windy"]

    df = standardise_string_columns(bronze_df)

    df = (
        df.withColumn("weather_date", F.to_date("weather_date"))
        .withColumn("avg_temperature_c", F.col("avg_temperature_c").cast("double"))
        .withColumn("min_temperature_c", F.col("min_temperature_c").cast("double"))
        .withColumn("max_temperature_c", F.col("max_temperature_c").cast("double"))
        .withColumn("humidity_percent", F.col("humidity_percent").cast("double"))
        .withColumn("wind_speed_mph", F.col("wind_speed_mph").cast("double"))
        .withColumn("heating_degree_days", F.col("heating_degree_days").cast("double"))
        .withColumn("cooling_degree_days", F.col("cooling_degree_days").cast("double"))
        .withColumn("generated_at_utc", F.to_timestamp("generated_at_utc"))
        .withColumn("_ingested_at_utc", F.to_timestamp("_ingested_at_utc"))
    )

    df = add_duplicate_flag(df, "weather_id", "is_duplicate_weather_id")

    df = (
        df.withColumn("is_missing_weather_id", F.col("weather_id").isNull() | (F.col("weather_id") == ""))
        .withColumn("is_missing_weather_date", F.col("weather_date").isNull())
        .withColumn("is_missing_region", F.col("region").isNull() | (F.col("region") == ""))
        .withColumn("is_invalid_avg_temperature", F.col("avg_temperature_c").isNull() | (F.col("avg_temperature_c") < -30) | (F.col("avg_temperature_c") > 45))
        .withColumn("is_invalid_temperature_range", F.col("max_temperature_c") < F.col("min_temperature_c"))
        .withColumn("is_invalid_humidity", F.col("humidity_percent").isNull() | (F.col("humidity_percent") < 0) | (F.col("humidity_percent") > 100))
        .withColumn("is_invalid_wind_speed", F.col("wind_speed_mph").isNull() | (F.col("wind_speed_mph") < 0) | (F.col("wind_speed_mph") > 100))
        .withColumn("is_invalid_weather_condition", ~F.col("weather_condition").isin(*valid_conditions))
    )

    reason_map = {
        "is_missing_weather_id": "Missing weather_id",
        "is_missing_weather_date": "Missing weather_date",
        "is_missing_region": "Missing region",
        "is_invalid_avg_temperature": "Average temperature outside expected range",
        "is_invalid_temperature_range": "max_temperature_c lower than min_temperature_c",
        "is_invalid_humidity": "Humidity outside expected range 0-100",
        "is_invalid_wind_speed": "Wind speed outside expected range",
        "is_invalid_weather_condition": "Invalid weather_condition",
        "is_duplicate_weather_id": "Duplicate weather_id",
    }

    return df, reason_map, "is_duplicate_weather_id"


def transform_outage_events(bronze_df: DataFrame):
    valid_severities = ["Low", "Medium", "High", "Critical"]
    valid_outage_types = [
        "Planned Maintenance",
        "Network Fault",
        "Storm Damage",
        "Equipment Failure",
        "Third Party Damage",
    ]

    df = standardise_string_columns(bronze_df)

    df = (
        df.withColumn("outage_start_timestamp", F.to_timestamp("outage_start_timestamp"))
        .withColumn("outage_end_timestamp", F.to_timestamp("outage_end_timestamp"))
        .withColumn("duration_minutes", F.col("duration_minutes").cast("double"))
        .withColumn("affected_customers", F.col("affected_customers").cast("long"))
        .withColumn("resolved_flag", F.col("resolved_flag").cast("boolean"))
        .withColumn("generated_at_utc", F.to_timestamp("generated_at_utc"))
        .withColumn("_ingested_at_utc", F.to_timestamp("_ingested_at_utc"))
    )

    df = add_duplicate_flag(df, "outage_id", "is_duplicate_outage_id")

    df = (
        df.withColumn("is_missing_outage_id", F.col("outage_id").isNull() | (F.col("outage_id") == ""))
        .withColumn("is_missing_region", F.col("region").isNull() | (F.col("region") == ""))
        .withColumn("is_invalid_outage_type", ~F.col("outage_type").isin(*valid_outage_types))
        .withColumn("is_invalid_severity", ~F.col("severity").isin(*valid_severities))
        .withColumn("is_missing_start_timestamp", F.col("outage_start_timestamp").isNull())
        .withColumn("is_missing_end_timestamp", F.col("outage_end_timestamp").isNull())
        .withColumn(
            "is_invalid_timestamp_range",
            F.col("outage_end_timestamp").isNotNull()
            & F.col("outage_start_timestamp").isNotNull()
            & (F.col("outage_end_timestamp") < F.col("outage_start_timestamp")),
        )
        .withColumn("is_invalid_duration", F.col("duration_minutes").isNull() | (F.col("duration_minutes") <= 0))
        .withColumn("is_invalid_affected_customers", F.col("affected_customers").isNull() | (F.col("affected_customers") < 0))
    )

    reason_map = {
        "is_missing_outage_id": "Missing outage_id",
        "is_missing_region": "Missing region",
        "is_invalid_outage_type": "Invalid outage_type",
        "is_invalid_severity": "Invalid severity",
        "is_missing_start_timestamp": "Missing outage_start_timestamp",
        "is_missing_end_timestamp": "Missing outage_end_timestamp",
        "is_invalid_timestamp_range": "outage_end_timestamp earlier than outage_start_timestamp",
        "is_invalid_duration": "Invalid duration_minutes",
        "is_invalid_affected_customers": "Invalid affected_customers",
        "is_duplicate_outage_id": "Duplicate outage_id",
    }

    return df, reason_map, "is_duplicate_outage_id"


def transform_billing_events(bronze_df: DataFrame):
    valid_payment_statuses = ["Paid", "Pending", "Failed", "Overdue"]
    valid_payment_methods = ["Direct Debit", "Card", "Bank Transfer", "Prepayment"]

    customers_clean_df = spark.table(f"{catalog}.{silver_schema}.customers_clean")
    tariffs_clean_df = spark.table(f"{catalog}.{silver_schema}.tariffs_clean")

    df = standardise_string_columns(bronze_df)

    df = (
        df.withColumn("billing_date", F.to_date("billing_date"))
        .withColumn("billing_period_start", F.to_date("billing_period_start"))
        .withColumn("billing_period_end", F.to_date("billing_period_end"))
        .withColumn("due_date", F.to_date("due_date"))
        .withColumn("paid_at", F.to_date("paid_at"))
        .withColumn("total_consumption_kwh", F.col("total_consumption_kwh").cast("double"))
        .withColumn("standing_charge_amount", F.col("standing_charge_amount").cast("double"))
        .withColumn("energy_charge_amount", F.col("energy_charge_amount").cast("double"))
        .withColumn("vat_amount", F.col("vat_amount").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("billing_quality_issue_injected", F.col("billing_quality_issue_injected").cast("boolean"))
        .withColumn("generated_at_utc", F.to_timestamp("generated_at_utc"))
        .withColumn("_ingested_at_utc", F.to_timestamp("_ingested_at_utc"))
    )

    df = add_duplicate_flag(df, "billing_event_id", "is_duplicate_billing_event_id")

    df = add_reference_check(
        df=df,
        reference_df=customers_clean_df,
        join_col="customer_id",
        flag_col="is_unknown_customer_id",
    )

    df = add_reference_check(
        df=df,
        reference_df=tariffs_clean_df,
        join_col="tariff_id",
        flag_col="is_unknown_tariff_id",
    )

    df = (
        df.withColumn("is_missing_billing_event_id", F.col("billing_event_id").isNull() | (F.col("billing_event_id") == ""))
        .withColumn("is_missing_customer_id", F.col("customer_id").isNull() | (F.col("customer_id") == ""))
        .withColumn("is_missing_tariff_id", F.col("tariff_id").isNull() | (F.col("tariff_id") == ""))
        .withColumn("is_missing_billing_date", F.col("billing_date").isNull())
        .withColumn("is_invalid_total_consumption", F.col("total_consumption_kwh").isNull() | (F.col("total_consumption_kwh") < 0))
        .withColumn("is_negative_total_amount", F.col("total_amount") < 0)
        .withColumn("is_high_total_amount", F.col("total_amount") > 1000)
        .withColumn("is_missing_total_amount", F.col("total_amount").isNull())
        .withColumn("is_invalid_payment_status", ~F.col("payment_status").isin(*valid_payment_statuses))
        .withColumn("is_invalid_payment_method", ~F.col("payment_method").isin(*valid_payment_methods))
        .withColumn(
            "is_invalid_billing_period",
            F.col("billing_period_end").isNotNull()
            & F.col("billing_period_start").isNotNull()
            & (F.col("billing_period_end") < F.col("billing_period_start")),
        )
        .withColumn(
            "is_paid_without_paid_at",
            (F.col("payment_status") == "Paid") & F.col("paid_at").isNull(),
        )
    )

    reason_map = {
        "is_missing_billing_event_id": "Missing billing_event_id",
        "is_missing_customer_id": "Missing customer_id",
        "is_missing_tariff_id": "Missing tariff_id",
        "is_unknown_customer_id": "customer_id does not exist in customers_clean",
        "is_unknown_tariff_id": "tariff_id does not exist in tariffs_clean",
        "is_missing_billing_date": "Missing billing_date",
        "is_invalid_total_consumption": "Invalid total_consumption_kwh",
        "is_missing_total_amount": "Missing total_amount",
        "is_negative_total_amount": "Negative total_amount",
        "is_high_total_amount": "High total_amount greater than £1000",
        "is_invalid_payment_status": "Invalid payment_status",
        "is_invalid_payment_method": "Invalid payment_method",
        "is_invalid_billing_period": "billing_period_end earlier than billing_period_start",
        "is_paid_without_paid_at": "Paid billing event missing paid_at",
        "is_duplicate_billing_event_id": "Duplicate billing_event_id",
    }

    return df, reason_map, "is_duplicate_billing_event_id"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Process sources in dependency order

# COMMAND ----------

process_silver_table(
    source_name="customers",
    bronze_table_name="customers_raw",
    clean_table_name="customers_clean",
    quarantine_table_name="customers_quarantine",
    transform_function=transform_customers,
)

process_silver_table(
    source_name="tariffs",
    bronze_table_name="tariffs_raw",
    clean_table_name="tariffs_clean",
    quarantine_table_name="tariffs_quarantine",
    transform_function=transform_tariffs,
)

process_silver_table(
    source_name="meters",
    bronze_table_name="meters_raw",
    clean_table_name="meters_clean",
    quarantine_table_name="meters_quarantine",
    transform_function=transform_meters,
)

process_silver_table(
    source_name="weather",
    bronze_table_name="weather_raw",
    clean_table_name="weather_clean",
    quarantine_table_name="weather_quarantine",
    transform_function=transform_weather,
)

process_silver_table(
    source_name="outage_events",
    bronze_table_name="outage_events_raw",
    clean_table_name="outage_events_clean",
    quarantine_table_name="outage_events_quarantine",
    transform_function=transform_outage_events,
)

process_silver_table(
    source_name="billing_events",
    bronze_table_name="billing_events_raw",
    clean_table_name="billing_events_clean",
    quarantine_table_name="billing_events_quarantine",
    transform_function=transform_billing_events,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Show Silver summary

# COMMAND ----------

summary_queries = []

silver_tables = [
    "customers_clean",
    "customers_quarantine",
    "tariffs_clean",
    "tariffs_quarantine",
    "meters_clean",
    "meters_quarantine",
    "weather_clean",
    "weather_quarantine",
    "outage_events_clean",
    "outage_events_quarantine",
    "billing_events_clean",
    "billing_events_quarantine",
]

for table_name in silver_tables:
    full_table_name = f"{catalog}.{silver_schema}.{table_name}"
    row_count = spark.table(full_table_name).count()
    summary_queries.append((full_table_name, row_count))

summary_df = spark.createDataFrame(summary_queries, ["table_name", "row_count"])

display(summary_df.orderBy("table_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Show latest audit records

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT *
        FROM {catalog}.{audit_schema}.silver_processing_audit
        WHERE run_id = '{run_id}'
        ORDER BY started_at_utc
        """
    )
)