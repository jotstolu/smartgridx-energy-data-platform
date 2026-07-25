from datetime import datetime
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType, DateType


def create_silver_audit_table(spark: SparkSession, catalog: str,audit_schema: str,) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{audit_schema}.silver_processing_audit(
        run_id STRING,
        source_table STRING,
        clean_table STRING,
        quarantine_table STRING,
        status STRING,
        bronze_records BIGINT,
        clean_records BIGINT,
        quarantined_records BIGINT,
        duplicate_records BIGINT,
        error_nessage STRING,
        started_at_utc TIMESTAMP,
        ended_at_utc TIMESTANP,
        environment STRING
        )
        USING DELTA
        """
    )


def write_silver_audit_record(
    spark:SparkSession,
    catalog: str,
    audit_schema: str,
    audit_record : Dict,

) -> None:
    audit_df = spark.createDataFrame([audit_record])
    audit_df.write.format("delta").mode("append").saveAsTable(
        f"{catalog}.{audit_schema}.silver_processing_audit"
    )


def standardise_string_columns(df: DataFrame) -> DataFrame:
    result_df = df

    for column_name, data_type in result_df.dtypes:
        if data_type == "string":
            result_df = result_df.withColumn(
                column_name, F.trim(F.col(column_name))
            ) 
    
    return result_df


def cast_meter_reading_columns(df: DataFrame) -> DataFrame:
    """
    cast Bronze string columns into proper silver data types
    """

    return (
        df.withColumn("reading_timestamp", F.to_timestamp("reading_timestamp"))
        .withColumn("reading_date", F.to_date("reading_date"))
        .withColumn("consumption_kwh", F.col("consumption_kwh").cast(DoubleType()))
        .withColumn("voltage", F.col("voltage").cast(DoubleType()))
        .withColumn("ingestion_delay_hours", F.col("ingestion_delay_hours").cast(DoubleType()))
        .withColumn("signal_strength_dbm", F.col("signal_strength_dbm").cast(DoubleType()))
        .withColumn("generated_at_utc", F.to_timestamp("generated_at_utc"))
        .withColumn("_ingested_at_utc", F.col("_ingested_at_utc").cast(TimestampType()))
    )

def add_meter_reading_quality_flags(df: DataFrame) -> DataFrame:
    """
    Add data quality flags for meter readings.
    """
    current_ts = F.current_timestamp()

    return (
        df.withColumn(
            "is_missing_reading_id",
            F.col("reading_id").isNull() | (F.col("reading_id") == ""),
        )
        .withColumn(
            "is_missing_customer_id",
            F.col("customer_id").isNull() | (F.col("customer_id") == ""),
        )
        .withColumn(
            "is_missing_meter_id",
            F.col("meter_id").isNull() | (F.col("meter_id") == ""),
        )
        .withColumn(
            "is_negative_consumption",
            F.col("consumption_kwh") < 0,
        )
        .withColumn(
            "is_consumption_outlier",
            F.col("consumption_kwh") > 100,
        )
        .withColumn(
            "is_missing_consumption",
            F.col("consumption_kwh").isNull(),
        )
        .withColumn(
            "is_future_timestamp",
            F.col("reading_timestamp") > current_ts,
        )
        .withColumn(
            "is_invalid_voltage",
            (F.col("voltage") < 200) | (F.col("voltage") > 260),
        )
        .withColumn(
            "is_missing_reading_timestamp",
            F.col("reading_timestamp").isNull(),
        )
    )

def add_duplicate_flag(df:DataFrame) -> DataFrame:
    """
    flag duplicate readings

    """
    window_spec = Window.partitionBy("reading_id").orderBy(F.col("_ingested_at_utc").desc())

    return (
        df.withColumn("duplicate_rank", F.row_number().over(window_spec))
        .withColumn(
            "is_duplicate_reading",
            (F.col("reading_id").isNotNull()) & (F.col("duplicate_rank") > 1),
        )
    )

def add_quarantine_reason(df: DataFrame) -> DataFrame:
   
    return df.withColumn(
        "quarantine_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("is_missing_reading_id"), F.lit("Missing reading_id")),
            F.when(F.col("is_missing_customer_id"), F.lit("Missing customer_id")),
            F.when(F.col("is_missing_meter_id"), F.lit("Missing meter_id")),
            F.when(F.col("is_missing_reading_timestamp"), F.lit("Missing or invalid reading_timestamp")),
            F.when(F.col("is_missing_consumption"), F.lit("Missing or invalid consumption_kwh")),
            F.when(F.col("is_negative_consumption"), F.lit("Negative consumption_kwh")),
            F.when(F.col("is_consumption_outlier"), F.lit("Consumption outlier greater than 100 kWh")),
            F.when(F.col("is_future_timestamp"), F.lit("Future reading_timestamp")),
            F.when(F.col("is_invalid_voltage"), F.lit("Voltage outside expected range 200-260")),
            F.when(F.col("is_duplicate_reading"), F.lit("Duplicate reading_id")),
        ),
    )


def split_clean_and_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    split dataframe into clean and quarantined records.

    """
    has_quality_issue = (
        F.col("is_missing_reading_id")
        | F.col("is_missing_customer_id")
        | F.col("is_missing_meter_id")
        | F.col("is_missing_reading_timestamp")
        | F.col("is_missing_consumption")
        | F.col("is_negative_consumption")
        | F.col("is_consumption_outlier")
        | F.col("is_future_timestamp")
        | F.col("is_invalid_voltage")
        | F.col("is_duplicate_reading")
    )

    quarantine_df = df.filter(has_quality_issue)

    clean_df = df.filter(~has_quality_issue).drop("duplicate_rank")

    quarantine_df = quarantinne_df.withColumn(
        "quarantined_at_utc",
        F.current_timestamp()
    )

    clean_df = clean_df.withColumn(
        "silver_processed_at_utc",
        F.current_timestamp()
    )

    return clean_df, quarantine_df


def write_delta_table(df:DataFrame, catalog: str, schema_name: str, table_name: str, mode: str = "overwrite"
) -> None:
"""
write dataframe as a table

"""
    (
    df.write.format("delta")
    .mode(mode).option("overwriteSchem0-  a", "true")
    .saveAsTable(f"{catalog}.{schema_name}.{table_name}")
    )






