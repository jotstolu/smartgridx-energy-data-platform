{{
    config(
        materialized='incremental',
        unique_key='meter_reading_sk',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with readings as (

    select *
    from {{ ref('stg_meter_readings') }}

    {% if is_incremental() %}
        where silver_processed_at_utc >= (
            select coalesce(max(silver_processed_at_utc), timestamp('1900-01-01'))
            from {{ this }}
        )
    {% endif %}

),

meters as (
    select
        meter_id,
        meter_sk,
        customer_sk,
        tariff_sk
    from {{ ref('dim_meters') }}
),

regions as (
    select
        region,
        region_sk
    from {{ ref('dim_regions') }}
),

dates as (
    select
        date_day,
        date_sk
    from {{ ref('dim_date') }}
)

select
    r.meter_reading_sk,
    m.meter_sk,
    m.customer_sk,
    m.tariff_sk,
    reg.region_sk,
    d.date_sk,
    r.reading_id,
    r.meter_id,
    r.customer_id,
    r.reading_timestamp,
    r.reading_date,
    r.consumption_kwh,
    r.voltage,
    r.reading_source,
    r.meter_status,
    r.region,
    r.is_late_arriving,
    r.ingestion_delay_hours,
    r.firmware_version,
    r.signal_strength_dbm,
    r.meter_reading_quality_code,
    r.silver_processed_at_utc,
    {{ add_model_audit_columns() }}
from readings r
left join meters m
    on r.meter_id = m.meter_id
left join regions reg
    on r.region = reg.region
left join dates d
    on r.reading_date = d.date_day