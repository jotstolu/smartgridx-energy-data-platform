from pathlib import Path
import random
import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

from data_generator.utils.common import (
    PROJECT_ROOT,
    load_config,
    write_csv,
    add_ingestion_metadata,
)


fake = Faker("en_GB")


def generate_customers(config: dict) -> pd.DataFrame:
    random.seed(config["generation"]["random_seed"])
    np.random.seed(config["generation"]["random_seed"])
    Faker.seed(config["generation"]["random_seed"])

    number_of_customers = config["generation"]["number_of_customers"]

    customers = []

    for i in range(1, number_of_customers + 1):
        customer_id = f"CUST{i:06d}"
        region = random.choice(config["regions"])
        customer_segment = random.choice(config["customer_segments"])
        property_type = random.choice(config["property_types"])
        account_status = random.choices(
            config["account_statuses"],
            weights=[0.88, 0.07, 0.05],
            k=1,
        )[0]

        registration_date = fake.date_between(
            start_date="-5y",
            end_date="-30d",
        )

        customers.append(
            {
                "customer_id": customer_id,
                "customer_name": fake.name(),
                "email": fake.email(),
                "phone_number": fake.phone_number(),
                "postcode": fake.postcode(),
                "region": region,
                "customer_segment": customer_segment,
                "property_type": property_type,
                "account_status": account_status,
                "registration_date": registration_date,
            }
        )

    df = pd.DataFrame(customers)
    return df


def generate_tariffs(config: dict) -> pd.DataFrame:
    tariffs = []

    for tariff in config["tariffs"]:
        tariffs.append(
            {
                "tariff_id": tariff["tariff_id"],
                "tariff_name": tariff["tariff_name"],
                "standing_charge_pence_per_day": tariff["standing_charge_pence_per_day"],
                "unit_rate_pence_per_kwh": tariff["unit_rate_pence_per_kwh"],
                "green_energy_flag": tariff["green_energy_flag"],
                "effective_start_date": "2025-01-01",
                "effective_end_date": None,
            }
        )

    return pd.DataFrame(tariffs)


def generate_meters(customers_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    meters = []

    min_meters = config["generation"]["meters_per_customer_min"]
    max_meters = config["generation"]["meters_per_customer_max"]

    tariff_ids = [tariff["tariff_id"] for tariff in config["tariffs"]]

    for _, customer in customers_df.iterrows():
        number_of_meters = random.randint(min_meters, max_meters)

        for meter_number in range(1, number_of_meters + 1):
            meter_id = f"MTR-{customer['customer_id'][-6:]}-{meter_number}"

            meters.append(
                {
                    "meter_id": meter_id,
                    "customer_id": customer["customer_id"],
                    "tariff_id": random.choice(tariff_ids),
                    "region": customer["region"],
                    "meter_type": random.choice(["SMETS1", "SMETS2"]),
                    "meter_status": random.choices(
                        config["meter_statuses"],
                        weights=[0.93, 0.05, 0.02],
                        k=1,
                    )[0],
                    "installation_date": fake.date_between(
                        start_date=customer["registration_date"],
                        end_date="today",
                    ),
                }
            )

    return pd.DataFrame(meters)


def get_consumption_profile(customer_segment: str, timestamp: datetime) -> float:
    """
    Generate realistic-ish half-hourly consumption based on customer segment and time of day.
    """
    hour = timestamp.hour

    if customer_segment == "Residential":
        base = np.random.normal(0.35, 0.12)

        if 6 <= hour <= 8:
            base *= 1.8
        elif 17 <= hour <= 21:
            base *= 2.4
        elif 0 <= hour <= 5:
            base *= 0.45

    elif customer_segment == "Small Business":
        base = np.random.normal(0.9, 0.25)

        if 8 <= hour <= 18:
            base *= 2.2
        else:
            base *= 0.5

    elif customer_segment == "Commercial":
        base = np.random.normal(2.5, 0.6)

        if 7 <= hour <= 19:
            base *= 2.5
        else:
            base *= 0.6

    else:
        base = np.random.normal(1.8, 0.4)

        if 7 <= hour <= 18:
            base *= 2.0
        else:
            base *= 0.7

    return round(max(base, 0.01), 3)


def inject_data_quality_issues(readings_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Intentionally introduce common real-world data quality issues.
    """
    df = readings_df.copy()
    dq_config = config["data_quality_injection"]
    total_rows = len(df)

    # 1. Duplicate readings
    duplicate_count = int(total_rows * dq_config["duplicate_reading_rate"])
    if duplicate_count > 0:
        duplicates = df.sample(n=duplicate_count, random_state=42)
        df = pd.concat([df, duplicates], ignore_index=True)

    # 2. Negative consumption
    negative_count = int(total_rows * dq_config["negative_consumption_rate"])
    if negative_count > 0:
        negative_indexes = df.sample(n=negative_count, random_state=43).index
        df.loc[negative_indexes, "consumption_kwh"] = -abs(
            df.loc[negative_indexes, "consumption_kwh"]
        )

    # 3. Missing customer IDs
    missing_customer_count = int(total_rows * dq_config["missing_customer_id_rate"])
    if missing_customer_count > 0:
        missing_customer_indexes = df.sample(n=missing_customer_count, random_state=44).index
        df.loc[missing_customer_indexes, "customer_id"] = None

    # 4. Missing meter IDs
    missing_meter_count = int(total_rows * dq_config["missing_meter_id_rate"])
    if missing_meter_count > 0:
        missing_meter_indexes = df.sample(n=missing_meter_count, random_state=45).index
        df.loc[missing_meter_indexes, "meter_id"] = None

    # 5. Future timestamps
    future_count = int(total_rows * dq_config["future_timestamp_rate"])
    if future_count > 0:
        future_indexes = df.sample(n=future_count, random_state=46).index
        df.loc[future_indexes, "reading_timestamp"] = pd.Timestamp.now() + pd.Timedelta(days=5)

    # 6. Outlier consumption
    outlier_count = int(total_rows * dq_config["outlier_consumption_rate"])
    if outlier_count > 0:
        outlier_indexes = df.sample(n=outlier_count, random_state=47).index
        df.loc[outlier_indexes, "consumption_kwh"] = np.random.uniform(100, 500, size=outlier_count).round(3)

    # 7. Late-arriving records
    late_count = int(total_rows * dq_config["late_arriving_rate"])
    df["is_late_arriving"] = False
    df["reading_timestamp"] = pd.to_datetime(df["reading_timestamp"], errors="coerce")

    if late_count > 0:
        late_indexes = df.sample(n=late_count, random_state=48).index
        df.loc[late_indexes, "is_late_arriving"] = True
        df.loc[late_indexes, "ingestion_delay_hours"] = np.random.randint(24, 120, size=late_count)

    df["data_quality_issue_injected"] = False
    df.loc[
        (df["consumption_kwh"] < 0)
        | (df["customer_id"].isna())
        | (df["meter_id"].isna())
        | (df["reading_timestamp"] > pd.Timestamp.now())
        | (df["consumption_kwh"] > 100),
        "data_quality_issue_injected",
    ] = True

    return df


def generate_meter_readings(
    customers_df: pd.DataFrame,
    meters_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    start_date = datetime.fromisoformat(config["generation"]["start_date"])
    end_date = datetime.fromisoformat(config["generation"]["end_date"])
    frequency_minutes = config["generation"]["reading_frequency_minutes"]

    customer_lookup = customers_df.set_index("customer_id")["customer_segment"].to_dict()

    readings = []
    current_timestamp = start_date

    while current_timestamp <= end_date:
        for _, meter in meters_df.iterrows():
            customer_id = meter["customer_id"]
            customer_segment = customer_lookup.get(customer_id, "Residential")

            reading_id = str(uuid.uuid4())
            consumption_kwh = get_consumption_profile(customer_segment, current_timestamp)

            readings.append(
                {
                    "reading_id": reading_id,
                    "meter_id": meter["meter_id"],
                    "customer_id": customer_id,
                    "reading_timestamp": current_timestamp,
                    "reading_date": current_timestamp.date(),
                    "consumption_kwh": consumption_kwh,
                    "voltage": round(np.random.normal(230, 6), 2),
                    "reading_source": random.choice(["smart_meter", "estimated", "manual_adjustment"]),
                    "meter_status": meter["meter_status"],
                    "region": meter["region"],
                }
            )

        current_timestamp += timedelta(minutes=frequency_minutes)

    readings_df = pd.DataFrame(readings)
    readings_df = inject_data_quality_issues(readings_df, config)

    return readings_df

def is_schema_drift_date(reading_date, config: dict) -> bool:
    """
    Decide whether schema drift should be applied for a given reading date.
    """
    schema_drift_config = config.get("schema_drift", {})

    if not schema_drift_config.get("enabled", False):
        return False

    drift_start_date = datetime.fromisoformat(
        schema_drift_config["drift_start_date"]
    ).date()

    return reading_date >= drift_start_date


def apply_schema_drift(
    day_df: pd.DataFrame,
    reading_date,
    config: dict,
) -> pd.DataFrame:
    """
    Simulate source schema drift by adding new columns from a chosen date onwards.
    """
    df = day_df.copy()

    if not is_schema_drift_date(reading_date, config):
        return df

    schema_drift_config = config["schema_drift"]

    random_seed = config["generation"]["random_seed"] + int(
        reading_date.strftime("%Y%m%d")
    )

    rng = np.random.default_rng(random_seed)

    df["firmware_version"] = rng.choice(
        schema_drift_config["firmware_versions"],
        size=len(df),
    )

    df["signal_strength_dbm"] = rng.normal(
        loc=-70,
        scale=8,
        size=len(df),
    ).round(2)

    df["meter_reading_quality_code"] = rng.choice(
        schema_drift_config["quality_codes"],
        size=len(df),
        p=[0.88, 0.09, 0.03],
    )

    return df


def write_meter_readings_by_day(
    readings_df: pd.DataFrame,
    output_base: Path,
    config: dict,
) -> pd.DataFrame:
   
    df = readings_df.copy()
    df["reading_date"] = pd.to_datetime(df["reading_date"]).dt.date

    manifest_records = []

    for reading_date, day_df in df.groupby("reading_date"):
        day_df = day_df.copy()
        day_df = apply_schema_drift(day_df, reading_date, config)

        file_date = reading_date.strftime("%Y%m%d")
        partition_folder = f"reading_date={reading_date.isoformat()}"
        file_name = f"meter_readings_{file_date}.csv"

        output_path = (
            output_base
            / "meter_readings"
            / partition_folder
            / file_name
        )

        day_df = add_ingestion_metadata(day_df, file_name)
        write_csv(day_df, output_path)

        schema_drift_applied = is_schema_drift_date(reading_date, config)

        manifest_records.append(
            {
                "source_name": "meter_readings",
                "file_name": file_name,
                "file_path": str(output_path.relative_to(PROJECT_ROOT)),
                "reading_date": reading_date.isoformat(),
                "record_count": len(day_df),
                "column_count": len(day_df.columns),
                "schema_drift_applied": schema_drift_applied,
                "columns": "|".join(day_df.columns),
                "generated_at_utc": pd.Timestamp.utcnow(),
            }
        )

    manifest_df = pd.DataFrame(manifest_records)

    manifest_path = (
        PROJECT_ROOT
        / "data"
        / "metadata"
        / "meter_readings_file_manifest.csv"
    )

    write_csv(manifest_df, manifest_path)

    return manifest_df


def generate_weather(config: dict) -> pd.DataFrame:
    """
    Generate daily regional weather data.

    Weather affects electricity consumption, especially heating demand in cold weather.
    """
    start_date = datetime.fromisoformat(config["generation"]["start_date"]).date()
    end_date = datetime.fromisoformat(config["generation"]["end_date"]).date()

    weather_records = []
    current_date = start_date

    while current_date <= end_date:
        month = current_date.month

        for region in config["regions"]:
            # Simple seasonal temperature profile
            if month in [12, 1, 2]:
                avg_temp = np.random.normal(5, 3)
            elif month in [3, 4, 5]:
                avg_temp = np.random.normal(11, 4)
            elif month in [6, 7, 8]:
                avg_temp = np.random.normal(18, 4)
            else:
                avg_temp = np.random.normal(10, 4)

            min_temp = round(avg_temp - np.random.uniform(2, 6), 2)
            max_temp = round(avg_temp + np.random.uniform(2, 6), 2)

            # Heating degree days: demand tends to rise when temp is below 15.5C
            heating_degree_days = round(max(0, 15.5 - avg_temp), 2)

            # Cooling degree days: demand may rise when temp is above 22C
            cooling_degree_days = round(max(0, avg_temp - 22), 2)

            weather_records.append(
                {
                    "weather_id": str(uuid.uuid4()),
                    "weather_date": current_date,
                    "region": region,
                    "avg_temperature_c": round(avg_temp, 2),
                    "min_temperature_c": min_temp,
                    "max_temperature_c": max_temp,
                    "humidity_percent": round(np.random.uniform(45, 95), 2),
                    "wind_speed_mph": round(np.random.uniform(2, 35), 2),
                    "weather_condition": random.choices(
                        config["weather"]["conditions"],
                        weights=[0.25, 0.30, 0.22, 0.08, 0.03, 0.05, 0.07],
                        k=1,
                    )[0],
                    "heating_degree_days": heating_degree_days,
                    "cooling_degree_days": cooling_degree_days,
                }
            )

        current_date += timedelta(days=1)

    return pd.DataFrame(weather_records)


def generate_outage_events(config: dict) -> pd.DataFrame:
    """
    Generate synthetic regional outage events.
    """
    start_date = datetime.fromisoformat(config["generation"]["start_date"]).date()
    end_date = datetime.fromisoformat(config["generation"]["end_date"]).date()

    outage_records = []
    current_date = start_date

    outage_probability = config["outage_events"]["outage_probability_per_region_per_day"]

    while current_date <= end_date:
        for region in config["regions"]:
            if random.random() <= outage_probability:
                start_hour = random.randint(0, 23)
                start_minute = random.choice([0, 15, 30, 45])
                duration_minutes = random.choice([30, 60, 90, 120, 180, 240, 360])

                outage_start = datetime.combine(
                    current_date,
                    datetime.min.time(),
                ) + timedelta(hours=start_hour, minutes=start_minute)

                outage_end = outage_start + timedelta(minutes=duration_minutes)

                severity = random.choices(
                    config["outage_events"]["severity_levels"],
                    weights=[0.45, 0.35, 0.15, 0.05],
                    k=1,
                )[0]

                if severity == "Low":
                    affected_customers = random.randint(10, 100)
                elif severity == "Medium":
                    affected_customers = random.randint(100, 800)
                elif severity == "High":
                    affected_customers = random.randint(800, 2500)
                else:
                    affected_customers = random.randint(2500, 8000)

                outage_records.append(
                    {
                        "outage_id": str(uuid.uuid4()),
                        "region": region,
                        "outage_type": random.choice(config["outage_events"]["outage_types"]),
                        "severity": severity,
                        "outage_start_timestamp": outage_start,
                        "outage_end_timestamp": outage_end,
                        "duration_minutes": duration_minutes,
                        "affected_customers": affected_customers,
                        "resolved_flag": random.choices(
                            [True, False],
                            weights=[0.92, 0.08],
                            k=1,
                        )[0],
                    }
                )

        current_date += timedelta(days=1)

    return pd.DataFrame(outage_records)


def generate_billing_events(
    readings_df: pd.DataFrame,
    meters_df: pd.DataFrame,
    tariffs_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """
    Generate daily billing events from valid meter readings.

    This simulates how an energy supplier may calculate usage-based charges.
    """
    valid_readings_df = readings_df.copy()

    valid_readings_df["reading_date"] = pd.to_datetime(
        valid_readings_df["reading_date"],
        errors="coerce",
    ).dt.date

    # Billing should not be based on obviously bad records.
    valid_readings_df = valid_readings_df[
        valid_readings_df["customer_id"].notna()
        & valid_readings_df["meter_id"].notna()
        & (valid_readings_df["consumption_kwh"] >= 0)
        & (valid_readings_df["consumption_kwh"] <= 100)
    ]

    meter_tariff_df = meters_df[
        ["meter_id", "customer_id", "tariff_id", "region"]
    ].drop_duplicates()

    tariff_rates_df = tariffs_df[
        [
            "tariff_id",
            "standing_charge_pence_per_day",
            "unit_rate_pence_per_kwh",
        ]
    ].drop_duplicates()

    enriched_df = valid_readings_df.merge(
        meter_tariff_df,
        on=["meter_id", "customer_id", "region"],
        how="left",
    )

    enriched_df = enriched_df.merge(
        tariff_rates_df,
        on="tariff_id",
        how="left",
    )

    daily_billing_df = (
        enriched_df.groupby(
            [
                "reading_date",
                "customer_id",
                "tariff_id",
                "region",
                "standing_charge_pence_per_day",
                "unit_rate_pence_per_kwh",
            ],
            dropna=False,
        )
        .agg(total_consumption_kwh=("consumption_kwh", "sum"))
        .reset_index()
    )

    billing_records = []
    vat_rate = config["billing"]["vat_rate"]

    for _, row in daily_billing_df.iterrows():
        consumption_kwh = round(row["total_consumption_kwh"], 3)

        energy_charge = round(
            consumption_kwh * row["unit_rate_pence_per_kwh"] / 100,
            2,
        )

        standing_charge = round(
            row["standing_charge_pence_per_day"] / 100,
            2,
        )

        subtotal = round(energy_charge + standing_charge, 2)
        vat_amount = round(subtotal * vat_rate, 2)
        total_amount = round(subtotal + vat_amount, 2)

        payment_status = random.choices(
            config["billing"]["payment_statuses"],
            weights=[0.72, 0.18, 0.04, 0.06],
            k=1,
        )[0]

        billing_date = row["reading_date"]
        due_date = billing_date + timedelta(days=14)

        if payment_status == "Paid":
            paid_at = billing_date + timedelta(days=random.randint(0, 10))
        else:
            paid_at = None

        billing_records.append(
            {
                "billing_event_id": str(uuid.uuid4()),
                "customer_id": row["customer_id"],
                "tariff_id": row["tariff_id"],
                "region": row["region"],
                "billing_date": billing_date,
                "billing_period_start": billing_date,
                "billing_period_end": billing_date,
                "total_consumption_kwh": consumption_kwh,
                "standing_charge_amount": standing_charge,
                "energy_charge_amount": energy_charge,
                "vat_amount": vat_amount,
                "total_amount": total_amount,
                "payment_status": payment_status,
                "payment_method": random.choice(config["billing"]["payment_methods"]),
                "due_date": due_date,
                "paid_at": paid_at,
            }
        )

    billing_df = pd.DataFrame(billing_records)

    billing_df = inject_billing_data_quality_issues(billing_df)

    return billing_df


def inject_billing_data_quality_issues(billing_df: pd.DataFrame) -> pd.DataFrame:
    """
    Inject realistic billing data quality issues.
    """
    df = billing_df.copy()

    if df.empty:
        return df

    total_rows = len(df)

    # Duplicate billing events
    duplicate_count = max(1, int(total_rows * 0.01))
    duplicates = df.sample(n=duplicate_count, random_state=101)
    df = pd.concat([df, duplicates], ignore_index=True)

    # Missing tariff_id
    missing_tariff_count = max(1, int(total_rows * 0.003))
    missing_tariff_indexes = df.sample(n=missing_tariff_count, random_state=102).index
    df.loc[missing_tariff_indexes, "tariff_id"] = None

    # Negative total_amount
    negative_amount_count = max(1, int(total_rows * 0.002))
    negative_amount_indexes = df.sample(n=negative_amount_count, random_state=103).index
    df.loc[negative_amount_indexes, "total_amount"] = -abs(
        df.loc[negative_amount_indexes, "total_amount"]
    )

    # Unrealistic high bills
    high_bill_count = max(1, int(total_rows * 0.002))
    high_bill_indexes = df.sample(n=high_bill_count, random_state=104).index
    df.loc[high_bill_indexes, "total_amount"] = np.random.uniform(
        1000,
        5000,
        size=high_bill_count,
    ).round(2)

    df["billing_quality_issue_injected"] = False

    df.loc[
        df["tariff_id"].isna()
        | (df["total_amount"] < 0)
        | (df["total_amount"] > 1000),
        "billing_quality_issue_injected",
    ] = True

    return df


def write_daily_source_files(
    df: pd.DataFrame,
    output_base: Path,
    source_name: str,
    date_column: str,
    file_prefix: str,
) -> pd.DataFrame:
    """
    Reusable function for writing daily source files.

    Example:
    data/raw/weather/event_date=2026-01-01/weather_20260101.csv
    """
    source_df = df.copy()
    source_df[date_column] = pd.to_datetime(
        source_df[date_column],
        errors="coerce",
    ).dt.date

    manifest_records = []

    for event_date, day_df in source_df.groupby(date_column):
        file_date = event_date.strftime("%Y%m%d")
        partition_folder = f"{date_column}={event_date.isoformat()}"
        file_name = f"{file_prefix}_{file_date}.csv"

        output_path = output_base / source_name / partition_folder / file_name

        day_df = add_ingestion_metadata(day_df, file_name)
        write_csv(day_df, output_path)

        manifest_records.append(
            {
                "source_name": source_name,
                "file_name": file_name,
                "file_path": str(output_path.relative_to(PROJECT_ROOT)),
                "event_date": event_date.isoformat(),
                "record_count": len(day_df),
                "column_count": len(day_df.columns),
                "columns": "|".join(day_df.columns),
                "generated_at_utc": pd.Timestamp.utcnow(),
            }
        )

    manifest_df = pd.DataFrame(manifest_records)

    manifest_path = (
        PROJECT_ROOT
        / "data"
        / "metadata"
        / f"{source_name}_file_manifest.csv"
    )

    write_csv(manifest_df, manifest_path)

    return manifest_df




def main() -> None:
    config = load_config()

    output_base = PROJECT_ROOT / "data" / "raw"

    print("Generating customers...")
    customers_df = generate_customers(config)
    customers_df = add_ingestion_metadata(customers_df, "customers_20260101.csv")
    write_csv(customers_df, output_base / "customers" / "customers_20260101.csv")

    print("Generating tariffs...")
    tariffs_df = generate_tariffs(config)
    tariffs_df = add_ingestion_metadata(tariffs_df, "tariffs_20260101.csv")
    write_csv(tariffs_df, output_base / "tariffs" / "tariffs_20260101.csv")

    print("Generating meters...")
    meters_df = generate_meters(customers_df, config)
    meters_df = add_ingestion_metadata(meters_df, "meters_20260101.csv")
    write_csv(meters_df, output_base / "meters" / "meters_20260101.csv")

    print("Generating meter readings...")
    readings_df = generate_meter_readings(customers_df, meters_df, config)

    print("Writing daily partitioned meter reading files...")
    manifest_df = write_meter_readings_by_day(readings_df, output_base, config)


    print("Generating weather data...")
    weather_df = generate_weather(config)
    weather_manifest_df = write_daily_source_files(
        df=weather_df,
        output_base=output_base,
        source_name="weather",
        date_column="weather_date",
        file_prefix="weather",
    )

    print("Generating outage events...")
    outage_events_df = generate_outage_events(config)
    outage_manifest_df = write_daily_source_files(
        df=outage_events_df,
        output_base=output_base,
        source_name="outage_events",
        date_column="outage_start_timestamp",
        file_prefix="outage_events",
    )

    print("Generating billing events...")
    billing_events_df = generate_billing_events(
        readings_df=readings_df,
        meters_df=meters_df,
        tariffs_df=tariffs_df,
        config=config,
    )
    billing_manifest_df = write_daily_source_files(
        df=billing_events_df,
        output_base=output_base,
        source_name="billing_events",
        date_column="billing_date",
        file_prefix="billing_events",
    )


    print("Data generation completed successfully.")
    print(f"Customers: {len(customers_df):,}")
    print(f"Tariffs: {len(tariffs_df):,}")
    print(f"Meters: {len(meters_df):,}")
    print(f"Meter readings: {len(readings_df):,}")
    print(f"Weather records: {len(weather_df):,}")
    print(f"Outage events: {len(outage_events_df):,}")
    print(f"Billing events: {len(billing_events_df):,}")
    print(f"Daily meter reading files: {len(manifest_df):,}")
    print(f"Daily weather files: {len(weather_manifest_df):,}")
    print(f"Daily outage files: {len(outage_manifest_df):,}")
    print(f"Daily billing files: {len(billing_manifest_df):,}")
    print("Schema drift starts from:", config["schema_drift"]["drift_start_date"])

if __name__ == "__main__":
    main()