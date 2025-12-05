{% test unique_combination(model, columns=None, arguments=None) %}

    {# Support both:
       - old style: unique_combination: columns: [...]
       - new style: unique_combination: arguments: { columns: [...] }
    #}
    {% if columns is none and arguments is not none %}
        {% set columns = arguments.get('columns') %}
    {% endif %}

    {% if columns is none %}
        {{ exceptions.raise_compiler_error(
            "unique_combination test requires a 'columns' argument"
        ) }}
    {% endif %}

    WITH [Validation] AS (
        SELECT
            {% for column in columns %}
                {{ column }}{% if not loop.last %},{% endif %}
            {% endfor %}
        FROM {{ model }}
        GROUP BY
            {% for column in columns %}
                {{ column }}{% if not loop.last %},{% endif %}
            {% endfor %}
        HAVING COUNT(*) > 1
    )
    SELECT *
    FROM [Validation]

{% endtest %}
