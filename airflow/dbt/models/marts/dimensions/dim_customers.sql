{{ config(materialized='table') }}

select
    customer_sk,
    customer_id,
    customer_name,
    email,
    phone_number,
    postcode,
    region,
    customer_segment,
    property_type,
    account_status,
    registration_date,
    case
        when account_status = 'Active' then true
        else false
    end as is_active_customer,
    source_system,
    silver_processed_at_utc,
    {{ add_model_audit_columns() }}
from {{ ref('stg_customers') }}