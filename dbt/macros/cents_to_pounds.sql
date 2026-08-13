{% macro cents_to_pounds(column_name) %}
    round(cast({{ column_name }} as double) / 100, 2)
{% endmacro %}