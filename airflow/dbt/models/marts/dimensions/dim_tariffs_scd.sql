{{ config(materialized='table') }}

select
    dbt_scd_id as tariff_version_sk,
    tariff_sk,
    tariff_id,
    tariff_name,
    standing_charge_pence_per_day,
    standing_charge_amount_per_day,
    unit_rate_pence_per_kwh,
    unit_rate_amount_per_kwh,
    green_energy_flag,
    effective_start_date,
    effective_end_date,
    is_current_tariff,
    dbt_valid_from as valid_from_timestamp,
    dbt_valid_to as valid_to_timestamp,
    case
        when dbt_valid_to is null then true
        else false
    end as is_current_record,
    {{ add_model_audit_columns() }}
from {{ ref('snap_tariffs') }}