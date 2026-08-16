{{
    config(
        materialized='incremental',
        unique_key='outage_sk',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with outages as (

    select *
    from {{ ref('stg_outage_events') }}

    {% if is_incremental() %}
        where silver_processed_at_utc >= (
            select coalesce(max(silver_processed_at_utc), timestamp('1900-01-01'))
            from {{ this }}
        )
    {% endif %}

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
    o.outage_sk,
    r.region_sk,
    d.date_sk,
    o.outage_id,
    o.region,
    o.outage_type,
    o.severity,
    o.outage_start_timestamp,
    o.outage_end_timestamp,
    cast(o.outage_start_timestamp as date) as outage_date,
    o.duration_minutes,
    o.affected_customers,
    o.resolved_flag,
    o.silver_processed_at_utc,
    {{ add_model_audit_columns() }}
from outages o
left join regions r
    on o.region = r.region
left join dates d
    on cast(o.outage_start_timestamp as date) = d.date_day