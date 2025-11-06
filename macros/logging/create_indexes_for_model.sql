{% macro create_indexes_for_model() %}
    {# Only run on execution, not compile #}
    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {# 1) Start with any explicit indexes defined in config (schemas.yml or model config()) #}
    {% set indexes = config.get('indexes', []) %}

    {# 2) Pull meta for this model #}
    {% set node = graph.nodes[model.unique_id] %}
    {% set meta = node.meta or {} %}
    {% set table_type = meta.get('table_type') %}
    {% set business_keys = meta.get('business_keys', []) %}

    {# 3) Determine auto_business_key_index with defaulting logic #}
    {% if table_type == 'dimension' %}
        {% if 'auto_business_key_index' in meta %}
            {% set auto_bk_index = meta.get('auto_business_key_index') %}
        {% else %}
            {# Default is TRUE for dimensions when not specified #}
            {% set auto_bk_index = true %}
        {% endif %}
    {% else %}
        {% set auto_bk_index = false %}
    {% endif %}

    {# 4) If auto BK index is enabled and BKs exist, maybe add an auto BK index #}
    {% if auto_bk_index and business_keys | length > 0 %}
        {% set auto_idx_name = 'UX_' ~ this.identifier ~ '_' ~ (business_keys | join('_')) %}

        {# Check if an explicit index with this name already exists in config.indexes #}
        {% set has_override = false %}
        {% for ix in indexes %}
            {% if ix.get('name') == auto_idx_name %}
                {% set has_override = true %}
            {% endif %}
        {% endfor %}

        {% if not has_override %}
            {# No explicit override → add our default UNIQUE BK index #}
            {% set auto_idx = {
                "name": auto_idx_name,
                "columns": business_keys,
                "unique": true
            } %}
            {% set indexes = indexes + [auto_idx] %}
        {% else %}
            {# Explicit definition in schemas.yml wins; do nothing here #}
        {% endif %}
    {% endif %}

    {# 5) If we still have no indexes, bail #}
    {% if indexes | length == 0 %}
        {{ return('') }}
    {% endif %}

    {# 6) Emit CREATE INDEX IF NOT EXISTS for each index #}
    {% for idx in indexes %}
        {% set idx_name = idx.get('name') %}
        {% set cols = idx.get('columns', []) %}
        {% set unique = idx.get('unique', false) %}
        {% set include_cols = idx.get('include', []) %}
        {% set where_clause = idx.get('where') %}

        {% if cols | length == 0 %}
            {% do log("Skipping index with no columns on " ~ this, info=true) %}
            {% continue %}
        {% endif %}

        {% set col_list_sql = '[' ~ (cols | join('], [')) ~ ']' %}

        {% if include_cols | length > 0 %}
            {% set include_list_sql = '[' ~ (include_cols | join('], [')) ~ ']' %}
        {% else %}
            {% set include_list_sql = '' %}
        {% endif %}

        {% set sql %}
IF NOT EXISTS (
    SELECT 1
    FROM {{ this.database }}.sys.indexes
    WHERE name = N'{{ idx_name }}'
      AND object_id = OBJECT_ID(N'{{ this.schema }}.{{ this.identifier }}')
)
BEGIN
    CREATE {% if unique %}UNIQUE {% endif %}NONCLUSTERED INDEX [{{ idx_name }}]
        ON {{ this }} ({{ col_list_sql }})
        {% if include_list_sql != '' %}
            INCLUDE ({{ include_list_sql }})
        {% endif %}
        {% if where_clause is not none %}
            WHERE {{ where_clause }}
        {% endif %}
    ;
END;
        {% endset %}

        {{ log("Ensuring index " ~ idx_name ~ " on " ~ this, info=true) }}
        {{ run_query(sql) }}
    {% endfor %}
{% endmacro %}
