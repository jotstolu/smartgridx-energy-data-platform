{% macro generate_surrogate_key(columns) %}
    sha2(
        concat_ws(
            '||',
            {% for column in columns %}
                coalesce(cast({{ column }} as string), '__null__')
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        ),
        256
    )
{% endmacro %}