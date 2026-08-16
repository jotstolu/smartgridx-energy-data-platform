{{
    config(
        materialized='incremental',
        unique_key=['reading_date', 'region'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with meter_readings as (

    select *
    from {{ ref('fact_meter_readings') }}

    {% if is_incremental() %}
        where reading_date >= (
            select coalesce(max(reading_date), date('1900-01-01'))
            from {{ this }}
        )
    {% endif %}

),

weather as (
    select
        weather_date,
        region,
        avg_temperature_c,
        weather_condition,
        heating_degree_days,
        cooling_degree_days
    from {{ ref('fact_weather_daily') }}
),

billing as (
    select
        billing_date,
        region,
        sum(total_amount) as total_revenue_amount,
        sum(total_consumption_kwh) as billed_consumption_kwh,
        count(distinct billing_event_id) as billing_event_count
    from {{ ref('fact_billing_events') }}
    group by billing_date, region
),

daily_usage as (
    select
        reading_date,
        region,
        count(*) as reading_count,
        count(distinct meter_id) as active_meter_count,
        count(distinct customer_id) as active_customer_count,
        sum(consumption_kwh) as total_consumption_kwh,
        avg(consumption_kwh) as avg_consumption_kwh,
        max(consumption_kwh) as max_half_hour_consumption_kwh,
        avg(voltage) as avg_voltage,
        sum(case when is_late_arriving then 1 else 0 end) as late_arriving_reading_count
    from meter_readings
    group by reading_date, region
)

select
    {{ generate_surrogate_key(["u.reading_date", "u.region"]) }} as daily_energy_summary_sk,
    u.reading_date,
    u.region,
    u.reading_count,
    u.active_meter_count,
    u.active_customer_count,
    round(u.total_consumption_kwh, 3) as total_consumption_kwh,
    round(u.avg_consumption_kwh, 3) as avg_consumption_kwh,
    round(u.max_half_hour_consumption_kwh, 3) as max_half_hour_consumption_kwh,
    round(u.avg_voltage, 2) as avg_voltage,
    u.late_arriving_reading_count,
    w.avg_temperature_c,
    w.weather_condition,
    w.heating_degree_days,
    w.cooling_degree_days,
    coalesce(b.total_revenue_amount, 0) as total_revenue_amount,
    coalesce(b.billed_consumption_kwh, 0) as billed_consumption_kwh,
    coalesce(b.billing_event_count, 0) as billing_event_count,
    {{ add_model_audit_columns() }}
from daily_usage u
left join weather w
    on u.reading_date = w.weather_date
    and u.region = w.region
left join billing b
    on u.reading_date = b.billing_date
    and u.region = b.region