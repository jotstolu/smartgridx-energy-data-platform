{% snapshot snap_tariffs %}

{{
    config(
        target_schema='snapshots',
        unique_key='tariff_id',
        strategy='check',
        check_cols=[
            'tariff_name',
            'standing_charge_pence_per_day',
            'standing_charge_amount_per_day',
            'unit_rate_pence_per_kwh',
            'unit_rate_amount_per_kwh',
            'green_energy_flag',
            'effective_start_date',
            'effective_end_date',
            'is_current_tariff'
        ]
    )
}}

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
    is_current_tariff
from {{ ref('dim_tariffs') }}

{% endsnapshot %}