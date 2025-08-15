-- macros/admin/drop_model_tables.sql
--
-- Usage example:
-- dbt run-operation drop_model_tables --args '{"models": ["my_model"], "package_name": "my_package", "confirm": true}'
--

{% macro drop_model_tables(models=[], package_name=None, confirm=False) %}
    {# --- Safety gates --- #}
    {% if env_var('ALLOW_TABLE_DROP', '0') != '1' %}
        {{ log("Refusing to drop tables: ALLOW_TABLE_DROP env var not set to '1'.", info=True) }}
        {% do return(None) %}
    {% endif %}

    {% if not confirm %}
        {{ log("Refusing to drop tables: set confirm=True to proceed.", info=True) }}
        {% do return(None) %}
    {% endif %}

    {% set dropped   = [] %}
    {% set not_found = [] %}

    {% for model_name in models %}
        {# --- Resolve the model node --- #}
        {% set nodes = graph.nodes.values()
            | selectattr('resource_type', 'equalto', 'model')
            | selectattr('name', 'equalto', model_name)
            | list %}
        {% if package_name %}
            {% set nodes = nodes | selectattr('package_name', 'equalto', package_name) | list %}
        {% endif %}
        {% if nodes | length == 0 %}
            {{ log("Model not found: " ~ model_name, info=True) }}
            {% continue %}
        {% endif %}
        {% set node = nodes[0] %}

        {# --- Resolve final database/schema/identifier as dbt would --- #}
        {% set identifier = node.config.alias if node.config.alias else node.name %}
        {% set schema_    = node.config.schema if node.config.schema else target.schema %}
        {% set database_  = node.config.database if node.config.database else target.database %}

        {# Quoted pieces and FQN for messages --- #}
        {% set qq_db     = adapter.quote(database_) %}
        {% set qq_schema = adapter.quote(schema_) %}
        {% set qq_ident  = adapter.quote(identifier) %}
        {% set fqn = qq_db ~ "." ~ qq_schema ~ "." ~ qq_ident %}

        {# --- Existence check using three-part naming (works cross-database) --- #}
        {% set exists_sql %}
        SELECT 1
        FROM {{ qq_db }}.sys.tables t
        JOIN {{ qq_db }}.sys.schemas s ON s.schema_id = t.schema_id
        WHERE t.name = N'{{ identifier }}' AND s.name = N'{{ schema_ }}';
        {% endset %}
        {% set rs = run_query(exists_sql) %}
        {% set exists = (rs is not none) and (rs.rows | length) > 0 %}

        {% if exists %}
            {# --- Drop the fully-qualified table --- #}
            {% set drop_sql %}
            DROP TABLE {{ fqn }};
            {% endset %}
            {{ log("Dropping " ~ fqn, info=True) }}
            {% do run_query(drop_sql) %}
            {% do dropped.append(fqn) %}
        {% else %}
            {{ log("Not found (no action): " ~ fqn, info=True) }}
            {% do not_found.append(fqn) %}
        {% endif %}
    {% endfor %}

    {# --- Summaries --- #}
    {% if dropped | length > 0 %}
        {{ log("Dropped: " ~ (dropped | join(", ")), info=True) }}
    {% endif %}
    {% if not_found | length > 0 %}
        {{ log("Not found: " ~ (not_found | join(", ")), info=True) }}
    {% endif %}
{% endmacro %}
