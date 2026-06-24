from pathlib import Path
import pandas as pd

from data_generator.utils.common import PROJECT_ROOT


def main() -> None:
    raw_path = PROJECT_ROOT / "data" / "raw"

    customers_path = raw_path / "customers" / "customers_20260101.csv"
    tariffs_path = raw_path / "tariffs" / "tariffs_20260101.csv"
    meters_path = raw_path / "meters" / "meters_20260101.csv"
    readings_path = raw_path / "meter_readings" / "meter_readings_20260101_20260107.csv"

    customers_df = pd.read_csv(customers_path)
    tariffs_df = pd.read_csv(tariffs_path)
    meters_df = pd.read_csv(meters_path)
    readings_df = pd.read_csv(readings_path)

    print("\nGenerated Dataset Summary")
    print("-" * 50)
    print(f"Customers: {len(customers_df):,}")
    print(f"Tariffs: {len(tariffs_df):,}")
    print(f"Meters: {len(meters_df):,}")
    print(f"Meter readings: {len(readings_df):,}")

    print("\nData Quality Checks")
    print("-" * 50)
    print(f"Missing customer_id in readings: {readings_df['customer_id'].isna().sum():,}")
    print(f"Missing meter_id in readings: {readings_df['meter_id'].isna().sum():,}")
    print(f"Negative consumption readings: {(readings_df['consumption_kwh'] < 0).sum():,}")
    print(f"Outlier consumption readings > 100 kWh: {(readings_df['consumption_kwh'] > 100).sum():,}")
    print(f"Duplicate reading rows: {readings_df.duplicated().sum():,}")

    readings_df["reading_timestamp"] = pd.to_datetime(readings_df["reading_timestamp"], errors="coerce")
    future_count = (readings_df["reading_timestamp"] > pd.Timestamp.now()).sum()
    print(f"Future timestamp readings: {future_count:,}")

    print("\nSample Meter Readings")
    print("-" * 50)
    print(readings_df.head(10))


if __name__ == "__main__":
    main()