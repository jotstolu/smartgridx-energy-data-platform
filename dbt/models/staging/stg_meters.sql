select
    {{ generate_surrogate_key(["meter_id"]) }} as meter_sk,
    meter_id,
    customer_id,
    tariff_id,
    region,
    meter_type,
    meter_status,
    cast(installation_date as date) as installation_date,
    source_file_name,
    source_system,
    generated_at_utc,
    _source_file_path,
    _run_id,
    _environment,
    silver_processed_at_utc,
    {{ add_model_audit_columns() }}
from {{ source('silver', 'meters_clean') }}