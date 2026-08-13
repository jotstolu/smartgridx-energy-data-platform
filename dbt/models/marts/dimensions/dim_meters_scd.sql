{{ config(materialized='table') }}

select
    dbt_scd_id as meter_version_sk,
    meter_sk,
    meter_id,
    customer_sk,
    tariff_sk,
    customer_id,
    tariff_id,
    region,
    meter_type,
    meter_status,
    installation_date,
    is_active_meter,
    dbt_valid_from as valid_from_timestamp,
    dbt_valid_to as valid_to_timestamp,
    case
        when dbt_valid_to is null then true
        else false
    end as is_current_record,
    {{ add_model_audit_columns() }}
from {{ ref('snap_meters') }}