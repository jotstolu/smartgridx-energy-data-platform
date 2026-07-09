from datetime import datetime
from typing import Dict, List
from functools import reduce
from glob import glob

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

def load_yaml_config(config_path: str) -> Dict:
    """
        Load Yaml config from Databricks local workspace path or 
        driver path
    """
    with open(config_path, "r", encoding = "utf_8") as file:
        return yaml.safe_load(file)

def create_database_if_not_exists(spark:SparkSession,catalog: str,schema_name: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")

def add_bronze_metadata(df: DataFrame, source_name: str, run_id: str, environment:str) -> DataFrame:
    return (
        df.withColumn("_source_name", F.lit("source_name"))\
        .withColumn("_source_file_path", F.input_file_name())\
        .withColumn("_ingested_at_utc", F.current_timestamp())\
        .withColumn("_run_id", F.lit(run_id))\
        .withColumn("_environment", F.lit(environment))
    )

def read_raw_csv(spark:SparkSession, file_path:str, multiline: bool = False) -> DataFrame:
     return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", str(multiline).lower())
        .option("mode", "PERMISSIVE")
        .option("rescuedDataColumn", "_rescued_data")
        .load(file_path)
    )

def read_raw_csv_with_schema_evolution(
    spark: SparkSession,
    file_path_pattern: str,
    multiline: bool = False,
) -> DataFrame:
    """
    Read CSV files one-by-one and union them by column name.

    This handles schema drift where later files contain additional columns
    that earlier files do not have.
    """
    matching_files = sorted(glob(file_path_pattern))

    if not matching_files:
        raise FileNotFoundError(f"No files found for pattern: {file_path_pattern}")

    dataframes = []

    for file_path in matching_files:
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("multiLine", str(multiline).lower())
            .option("mode", "PERMISSIVE")
            .option("rescuedDataColumn", "_rescued_data")
            .load(file_path)
        )

        dataframes.append(df)

    combined_df = reduce(
        lambda left_df, right_df: left_df.unionByName(
            right_df,
            allowMissingColumns=True,
        ),
        dataframes,
    )

    return combined_df

def write_bronze_delta(df:DataFrame, catalog:str, schema_name: str, table_name:str, load_type:str) -> None:
    target_table = f"{catalog}.{schema_name}.{table_name}"
    writer = (df.write.format("delta").option("mergeSchema", "true"))
    if load_type == "full":
        (writer.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table))
    elif load_type == "incremental":
        writer.mode("append").saveAsTable(target_table)
    else:
        raise ValueError(f"Unsupported load_type: {load_type}")

def get_table_count(spark: SparkSession, catalog:str, schema_name: str, table_name:str) -> int:
    target_table = f"{catalog}.{schema_name}.{table_name}"
    return spark.table(target_table).count()

def create_audit_table(spark: SparkSession, catalog: str, audit_schema: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{audit_schema}.bronze_ingestion_audit(
            run_id STRING,
            source_name STRING,
            target_table STRING,
            source_path STRING,
            file_pattern STRING,
            load_type STRING,
            status STRING,
            records_loaded BIGINT,
            error_message STRING,
            started_at_utc TIMESTAMP,
            ended_at_utc TIMESTAMP,
            environment STRING
        )
        USING DELTA
        """
        )
    
def write_audit_record(spark: SparkSession, catalog:str, audit_schema: str, audit_record: dict) -> None:
    audit_df = spark.createDataFrame([audit_record])
    audit_df.write.format("delta").mode("append").saveAsTable(
        f"{catalog}.{audit_schema}.bronze_ingestion_audit"
    )