-- macros/admin/drop_model_tables.sql
--
-- Usage example:
-- dbt run-operation drop_model_tables --args '{"models": ["my_model"], "package_name": "my_package", "confirm": true}'
--

{% macro drop_model_tables(models=[], package_name=None, confirm=False) %}

    {# Safety gates: require env var and explicit confirm, and typically only in prod #}
    {% if env_var('ALLOW_TABLE_DROP', '0') != '1' %}
        {{ log("Refusing to drop tables: ALLOW_TABLE_DROP env var not set to '1'.", info=True) }}
        {% do return(None) %}
    {% endif %}

    {% if not confirm %}
        {{ log("Refusing to drop tables: set confirm=True to proceed.", info=True) }}
        {% do return(None) %}
    {% endif %}

    {% set dropped = [] %}

    {% for model_name in models %}
        {# Find the node by name (optionally package) #}
        {% set candidates = graph.nodes.values()
            | selectattr('resource_type', 'equalto', 'model')
            | selectattr('name', 'equalto', model_name)
            | list %}
        {% if package_name %}
            {% set candidates = candidates | selectattr('package_name', 'equalto', package_name) | list %}
        {% endif %}

        {% if candidates | length == 0 %}
            {{ log("Model not found: " ~ model_name, info=True) }}
            {% continue %}
        {% endif %}
        {% set node = candidates[0] %}

        {# Resolve schema & identifier the way dbt will materialize it #}
        {% set identifier = node.config.alias if node.config.alias else node.name %}
        {% set schema_ = node.config.schema if node.config.schema else target.schema %}

        {# Drop if exists #}
        {% set sql %}
        IF OBJECT_ID(N'{{ schema_ }}.{{ identifier }}', 'U') IS NOT NULL
        BEGIN
            DROP TABLE {{ adapter.quote(schema_) }}.{{ adapter.quote(identifier) }};
        END
        {% endset %}
        {{ log("Dropping " ~ schema_ ~ "." ~ identifier, info=True) }}
        {% do run_query(sql) %}
        {% do dropped.append(schema_ ~ "." ~ identifier) %}
    {% endfor %}

    {{ log("Dropped tables: " ~ (dropped | join(", ")), info=True) }}

{% endmacro %}

