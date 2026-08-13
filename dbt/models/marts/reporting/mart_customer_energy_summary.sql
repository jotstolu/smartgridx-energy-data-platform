{{ config(materialized='table') }}

with readings as (
    select *
    from {{ ref('fact_meter_readings') }}
),

billing as (
    select
        customer_id,
        sum(total_amount) as total_billed_amount,
        sum(total_consumption_kwh) as total_billed_consumption_kwh,
        count(distinct billing_event_id) as billing_event_count,
        sum(case when payment_status = 'Paid' then total_amount else 0 end) as paid_amount,
        sum(case when payment_status in ('Pending', 'Overdue', 'Failed') then total_amount else 0 end) as unpaid_or_problem_amount
    from {{ ref('fact_billing_events') }}
    group by customer_id
),

customers as (
    select *
    from {{ ref('dim_customers') }}
)

select
    c.customer_sk,
    c.customer_id,
    c.customer_name,
    c.region,
    c.customer_segment,
    c.property_type,
    c.account_status,
    count(distinct r.meter_id) as meter_count,
    count(*) as reading_count,
    round(sum(r.consumption_kwh), 3) as total_consumption_kwh,
    round(avg(r.consumption_kwh), 3) as avg_half_hour_consumption_kwh,
    round(max(r.consumption_kwh), 3) as max_half_hour_consumption_kwh,
    min(r.reading_date) as first_reading_date,
    max(r.reading_date) as latest_reading_date,
    coalesce(b.total_billed_amount, 0) as total_billed_amount,
    coalesce(b.total_billed_consumption_kwh, 0) as total_billed_consumption_kwh,
    coalesce(b.billing_event_count, 0) as billing_event_count,
    coalesce(b.paid_amount, 0) as paid_amount,
    coalesce(b.unpaid_or_problem_amount, 0) as unpaid_or_problem_amount,
    case
        when sum(r.consumption_kwh) > 1000 then true
        else false
    end as is_high_consumption_customer,
    {{ add_model_audit_columns() }}
from customers c
left join readings r
    on c.customer_id = r.customer_id
left join billing b
    on c.customer_id = b.customer_id
group by
    c.customer_sk,
    c.customer_id,
    c.customer_name,
    c.region,
    c.customer_segment,
    c.property_type,
    c.account_status,
    b.total_billed_amount,
    b.total_billed_consumption_kwh,
    b.billing_event_count,
    b.paid_amount,
    b.unpaid_or_problem_amount