{{ config(materialized='table') }}

with weather as (
    select *
    from {{ ref('stg_weather') }}
),

regions as (
    select region, region_sk
    from {{ ref('dim_regions') }}
),

dates as (
    select date_day, date_sk
    from {{ ref('dim_date') }}
)

select
    w.weather_sk,
    r.region_sk,
    d.date_sk,
    w.weather_id,
    w.weather_date,
    w.region,
    w.avg_temperature_c,
    w.min_temperature_c,
    w.max_temperature_c,
    w.humidity_percent,
    w.wind_speed_mph,
    w.weather_condition,
    w.heating_degree_days,
    w.cooling_degree_days,
    w.silver_processed_at_utc,
    {{ add_model_audit_columns() }}
from weather w
left join regions r
    on w.region = r.region
left join dates d
    on w.weather_date = d.date_day