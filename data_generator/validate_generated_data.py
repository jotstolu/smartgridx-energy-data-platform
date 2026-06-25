from pathlib import Path
import pandas as pd

from data_generator.utils.common import PROJECT_ROOT


def main() -> None:
    raw_path = PROJECT_ROOT / "data" / "raw"
    metadata_path = PROJECT_ROOT / "data" / "metadata"

    customers_path = raw_path / "customers" / "customers_20260101.csv"
    tariffs_path = raw_path / "tariffs" / "tariffs_20260101.csv"
    meters_path = raw_path / "meters" / "meters_20260101.csv"

    readings_files = sorted(
        (raw_path / "meter_readings").glob("reading_date=*/meter_readings_*.csv")
    )

    if not readings_files:
        raise FileNotFoundError(
            "No daily meter reading files found. Run python -m data_generator.run_generator first."
        )

    customers_df = pd.read_csv(customers_path)
    tariffs_df = pd.read_csv(tariffs_path)
    meters_df = pd.read_csv(meters_path)

    readings_df = pd.concat(
        [pd.read_csv(file, low_memory=False) for file in readings_files],
        ignore_index=True,
    )

    manifest_path = metadata_path / "meter_readings_file_manifest.csv"

    if manifest_path.exists():
        manifest_df = pd.read_csv(manifest_path)
    else:
        manifest_df = pd.DataFrame()

    print("\nGenerated Dataset Summary")
    print("-" * 50)
    print(f"Customers: {len(customers_df):,}")
    print(f"Tariffs: {len(tariffs_df):,}")
    print(f"Meters: {len(meters_df):,}")
    print(f"Meter readings: {len(readings_df):,}")
    print(f"Daily meter reading files: {len(readings_files):,}")

    print("\nDaily Files")
    print("-" * 50)
    for file in readings_files:
        print(file.relative_to(PROJECT_ROOT))

    print("\nData Quality Checks")
    print("-" * 50)
    print(f"Missing customer_id in readings: {readings_df['customer_id'].isna().sum():,}")
    print(f"Missing meter_id in readings: {readings_df['meter_id'].isna().sum():,}")
    print(f"Negative consumption readings: {(readings_df['consumption_kwh'] < 0).sum():,}")
    print(f"Outlier consumption readings > 100 kWh: {(readings_df['consumption_kwh'] > 100).sum():,}")
    print(f"Duplicate reading rows: {readings_df.duplicated().sum():,}")

    readings_df["reading_timestamp"] = pd.to_datetime(
        readings_df["reading_timestamp"],
        errors="coerce",
    )

    future_count = (readings_df["reading_timestamp"] > pd.Timestamp.now()).sum()
    print(f"Future timestamp readings: {future_count:,}")

    print("\nSchema Drift Checks")
    print("-" * 50)

    drift_columns = [
        "firmware_version",
        "signal_strength_dbm",
        "meter_reading_quality_code",
    ]

    for column in drift_columns:
        print(f"{column} present: {column in readings_df.columns}")

    if not manifest_df.empty:
        print("\nManifest Summary")
        print("-" * 50)
        print(manifest_df[[
            "file_name",
            "reading_date",
            "record_count",
            "column_count",
            "schema_drift_applied",
        ]])

    print("\nSample Meter Readings")
    print("-" * 50)
    print(readings_df.head(10))


if __name__ == "__main__":
    main()