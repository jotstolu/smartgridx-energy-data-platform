{% snapshot snap_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'customer_name',
            'email',
            'phone_number',
            'postcode',
            'region',
            'customer_segment',
            'property_type',
            'account_status'
        ]
    )
}}

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
    is_active_customer,
    silver_processed_at_utc
from {{ ref('dim_customers') }}

{% endsnapshot %}