{{ config(materialized='table') }}

with meters as (
    select *
    from {{ ref('stg_meters') }}
),

customers as (
    select
        customer_id,
        customer_sk
    from {{ ref('dim_customers') }}
),

tariffs as (
    select
        tariff_id,
        tariff_sk
    from {{ ref('dim_tariffs') }}
)

select
    m.meter_sk,
    m.meter_id,
    c.customer_sk,
    t.tariff_sk,
    m.customer_id,
    m.tariff_id,
    m.region,
    m.meter_type,
    m.meter_status,
    m.installation_date,
    case
        when m.meter_status = 'Active' then true
        else false
    end as is_active_meter,
    m.silver_processed_at_utc,
    {{ add_model_audit_columns() }}
from meters m
left join customers c
    on m.customer_id = c.customer_id
left join tariffs t
    on m.tariff_id = t.tariff_id