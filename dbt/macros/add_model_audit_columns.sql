{% macro add_model_audit_columns() %}
    current_timestamp() as dbt_loaded_at,
    '{{ invocation_id }}' as dbt_invocation_id
{% endmacro %}