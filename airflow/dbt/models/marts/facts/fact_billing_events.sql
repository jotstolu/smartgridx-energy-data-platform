{{
    config(
        materialized='incremental',
        unique_key='billing_event_sk',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with billing as (

    select *
    from {{ ref('stg_billing_events') }}

    {% if is_incremental() %}
        where silver_processed_at_utc >= (
            select coalesce(max(silver_processed_at_utc), timestamp('1900-01-01'))
            from {{ this }}
        )
    {% endif %}

),

customers as (
    select customer_id, customer_sk
    from {{ ref('dim_customers') }}
),

tariffs as (
    select tariff_id, tariff_sk
    from {{ ref('dim_tariffs') }}
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
    b.billing_event_sk,
    c.customer_sk,
    t.tariff_sk,
    r.region_sk,
    d.date_sk,
    b.billing_event_id,
    b.customer_id,
    b.tariff_id,
    b.region,
    b.billing_date,
    b.billing_period_start,
    b.billing_period_end,
    b.total_consumption_kwh,
    b.standing_charge_amount,
    b.energy_charge_amount,
    b.vat_amount,
    b.total_amount,
    b.payment_status,
    b.payment_method,
    b.due_date,
    b.paid_at,
    b.billing_quality_issue_injected,
    b.silver_processed_at_utc,
    {{ add_model_audit_columns() }}
from billing b
left join customers c
    on b.customer_id = c.customer_id
left join tariffs t
    on b.tariff_id = t.tariff_id
left join regions r
    on b.region = r.region
left join dates d
    on b.billing_date = d.date_day