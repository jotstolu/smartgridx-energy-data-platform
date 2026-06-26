from pathlib import Path
import pandas as pd

from data_generator.utils.common import PROJECT_ROOT


def read_daily_files(raw_path: Path, source_name: str, pattern: str) -> pd.DataFrame:
    files = sorted((raw_path / source_name).glob(pattern))

    if not files:
        return pd.DataFrame()

    return pd.concat(
        [pd.read_csv(file, low_memory=False) for file in files],
        ignore_index=True,
    )


def count_daily_files(raw_path: Path, source_name: str, pattern: str) -> int:
    return len(sorted((raw_path / source_name).glob(pattern)))


def main() -> None:
    raw_path = PROJECT_ROOT / "data" / "raw"
    metadata_path = PROJECT_ROOT / "data" / "metadata"

    customers_path = raw_path / "customers" / "customers_20260101.csv"
    tariffs_path = raw_path / "tariffs" / "tariffs_20260101.csv"
    meters_path = raw_path / "meters" / "meters_20260101.csv"

    customers_df = pd.read_csv(customers_path)
    tariffs_df = pd.read_csv(tariffs_path)
    meters_df = pd.read_csv(meters_path)

    readings_df = read_daily_files(
        raw_path,
        "meter_readings",
        "reading_date=*/meter_readings_*.csv",
    )

    weather_df = read_daily_files(
        raw_path,
        "weather",
        "weather_date=*/weather_*.csv",
    )

    outage_df = read_daily_files(
        raw_path,
        "outage_events",
        "outage_start_timestamp=*/outage_events_*.csv",
    )

    billing_df = read_daily_files(
        raw_path,
        "billing_events",
        "billing_date=*/billing_events_*.csv",
    )

    print("\nGenerated Dataset Summary")
    print("-" * 50)
    print(f"Customers: {len(customers_df):,}")
    print(f"Tariffs: {len(tariffs_df):,}")
    print(f"Meters: {len(meters_df):,}")
    print(f"Meter readings: {len(readings_df):,}")
    print(f"Weather records: {len(weather_df):,}")
    print(f"Outage events: {len(outage_df):,}")
    print(f"Billing events: {len(billing_df):,}")

    print("\nDaily Source Files")
    print("-" * 50)
    print(
        "Meter reading files:",
        count_daily_files(raw_path, "meter_readings", "reading_date=*/meter_readings_*.csv"),
    )
    print(
        "Weather files:",
        count_daily_files(raw_path, "weather", "weather_date=*/weather_*.csv"),
    )
    print(
        "Outage files:",
        count_daily_files(raw_path, "outage_events", "outage_start_timestamp=*/outage_events_*.csv"),
    )
    print(
        "Billing files:",
        count_daily_files(raw_path, "billing_events", "billing_date=*/billing_events_*.csv"),
    )

    print("\nMeter Reading Data Quality Checks")
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

    print("\nBilling Data Quality Checks")
    print("-" * 50)
    if not billing_df.empty:
        print(f"Missing tariff_id in billing: {billing_df['tariff_id'].isna().sum():,}")
        print(f"Negative billing amounts: {(billing_df['total_amount'] < 0).sum():,}")
        print(f"High billing amounts > £1000: {(billing_df['total_amount'] > 1000).sum():,}")
        print(f"Duplicate billing rows: {billing_df.duplicated().sum():,}")
    else:
        print("No billing data generated.")

    print("\nSchema Drift Checks")
    print("-" * 50)

    drift_columns = [
        "firmware_version",
        "signal_strength_dbm",
        "meter_reading_quality_code",
    ]

    for column in drift_columns:
        print(f"{column} present: {column in readings_df.columns}")

    print("\nManifest Files")
    print("-" * 50)
    manifest_files = sorted(metadata_path.glob("*_file_manifest.csv"))

    for manifest_file in manifest_files:
        manifest_df = pd.read_csv(manifest_file)
        print(f"{manifest_file.name}: {len(manifest_df):,} file records")

    print("\nSample Billing Events")
    print("-" * 50)
    if not billing_df.empty:
        print(billing_df.head(10))

    print("\nSample Weather Records")
    print("-" * 50)
    if not weather_df.empty:
        print(weather_df.head(10))

    print("\nSample Outage Events")
    print("-" * 50)
    if not outage_df.empty:
        print(outage_df.head(10))


if __name__ == "__main__":
    main()