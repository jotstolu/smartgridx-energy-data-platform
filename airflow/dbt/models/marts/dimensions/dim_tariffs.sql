{{ config(materialized='table') }}

select
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
    case
        when effective_end_date is null then true
        else false
    end as is_current_tariff,
    {{ add_model_audit_columns() }}
from {{ ref('stg_tariffs') }}