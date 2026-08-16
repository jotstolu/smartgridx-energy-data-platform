{{ config(materialized='table') }}

select
    dbt_scd_id as customer_version_sk,
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
    is_active_customer,
    dbt_valid_from as valid_from_timestamp,
    dbt_valid_to as valid_to_timestamp,
    case
        when dbt_valid_to is null then true
        else false
    end as is_current_record,
    {{ add_model_audit_columns() }}
from {{ ref('snap_customers') }}