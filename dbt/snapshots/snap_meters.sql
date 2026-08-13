{% snapshot snap_meters %}

{{
    config(
        target_schema='snapshots',
        unique_key='meter_id',
        strategy='check',
        check_cols=[
            'customer_id',
            'tariff_id',
            'region',
            'meter_type',
            'meter_status',
            'installation_date',
            'is_active_meter'
        ]
    )
}}

select
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
    silver_processed_at_utc
from {{ ref('dim_meters') }}

{% endsnapshot %}