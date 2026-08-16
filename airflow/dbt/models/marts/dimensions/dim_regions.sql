{{ config(materialized='table') }}

with regions as (

    select distinct region from {{ ref('stg_customers') }}
    union
    select distinct region from {{ ref('stg_meters') }}
    union
    select distinct region from {{ ref('stg_meter_readings') }}
    union
    select distinct region from {{ ref('stg_weather') }}
    union
    select distinct region from {{ ref('stg_outage_events') }}
    union
    select distinct region from {{ ref('stg_billing_events') }}

)

select
    {{ generate_surrogate_key(["region"]) }} as region_sk,
    region,
    {{ add_model_audit_columns() }}
from regions
where region is not null