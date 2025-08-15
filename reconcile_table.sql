-- macros/admin/reconcile_table.sql
{% macro reconcile_table(model_name, package_name=None,
                        execute_statements=True,
                        allow_drop_columns=False,
                        allow_narrowing=False) %}


    {# 1 - Resolve the model node #}
    {% set candidates = graph.nodes.values()
        | selectattr('resource_type','equalto','model')
        | selectattr('name','equalto', model_name)
        | list %}
    {% if package_name %}
        {% set candidates = candidates | selectattr('package_name','equalto', package_name) | list %}
    {% endif %}
    {% if candidates | length == 0 %}
        {{ exceptions.raise_compiler_error("Model not found: " ~ model_name) }}
    {% endif %}
    {% set node = candidates[0] %}

    {# 2 - Desired relation + YAML spec #}
    {% set identifier = node.config.alias if node.config.alias else node.name %}
    {% set schema_   = node.config.schema if node.config.schema else target.schema %}
    {% set relation = adapter.get_relation(database=target.database, schema=schema_, identifier=identifier) %}

    {% if not relation %}
        {{ log("Table does not exist; nothing to reconcile. (Creation handled elsewhere)", info=True) }}
        {% do return(None) %}
    {% endif %}

    {# Build desired spec from schema.yml #}
    {% set desired = {} %}
    {% for col in node.columns.values() %}
        {% set m = col.meta if col.meta is not none else {} %}
        {% set colspec = {
            'name': col.name,
            'data_type': (m.get('data_type') | lower),
            'nullable':  m.get('nullable', True),
            'default':   m.get('default'),
            'identity':  m.get('identity', False)
        } %}
        {% do desired.update({ (col.name | lower): colspec }) %}
    {% endfor %}

    {# 3 - Introspect actual columns #}
    {% set sql_actual %}
    SELECT
        c.name                               AS column_name,
        LOWER(
          CASE
            WHEN t.name IN ('varchar','char','varbinary','binary') AND c.max_length <> -1
              THEN CONCAT(t.name,'(',c.max_length,')')
            WHEN t.name IN ('nvarchar','nchar') AND c.max_length <> -1
              THEN CONCAT(t.name,'(', c.max_length/2 ,')')   -- nvarchar stores bytes
            WHEN t.name IN ('decimal','numeric')
              THEN CONCAT(t.name,'(',c.precision,',',c.scale,')')
            WHEN c.max_length = -1
              THEN CONCAT(t.name,'(max)')
            ELSE t.name
          END
        )                                     AS data_type,
        c.is_nullable                         AS is_nullable,
        c.is_identity                         AS is_identity,
        dc.name                               AS default_name,
        CAST(OBJECT_DEFINITION(dc.object_id) AS nvarchar(4000)) AS default_definition
    FROM sys.columns c
    JOIN sys.types   t  ON c.user_type_id = t.user_type_id
    LEFT JOIN sys.default_constraints dc
           ON dc.parent_object_id = c.object_id
          AND dc.parent_column_id = c.column_id
    WHERE c.object_id = OBJECT_ID(N'{{ schema_ }}.{{ identifier }}')
    ORDER BY c.column_id;
    {% endset %}

    {% set actual_rows = run_query(sql_actual) %}
    {% set actual = {} %}
    {% if actual_rows and actual_rows.rows %}
        {% for r in actual_rows.rows %}
            {% set colname = (r[0] | lower) %}
            {% set colinfo = {
                'name': r[0],
                'data_type': (r[1] | lower),
                'nullable': (True if r[2] == 1 else False),
                'identity': (True if r[3] == 1 else False),
                'default_name': r[4],
                'default_definition': (r[5] or '') | trim
            } %}
            {% do actual.update({ colname: colinfo }) %}
        {% endfor %}
    {% endif %}

    {# 4 - Diff and build statements #}
    {% set stmts = [] %}
    {% set table_qq = adapter.quote(schema_) ~ '.' ~ adapter.quote(identifier) %}

    {# Helper to add statements with logging #}
    {% macro _emit(sql_text) -%}
        {% do stmts.append(sql_text) %}
        {{ log("DDL: " ~ sql_text, info=True) }}
    {%- endmacro %}

    {# 4a - Add missing columns #}
    {% for cname, spec in desired.items() %}
        {% if actual.get(cname) is none %}
            {% set nullable = 'NULL' if spec.nullable else 'NOT NULL' %}
            {% set default_clause = '' %}
            {% if spec.default %}
                {% set default_name = 'DF_' ~ identifier ~ '_' ~ spec.name %}
                {% set default_clause = ' CONSTRAINT ' ~ adapter.quote(default_name) ~ ' DEFAULT (' ~ spec.default ~ ')' %}
            {% endif %}
            {% set with_values = ' WITH VALUES' if (spec.default and not spec.nullable) else '' %}
            {% call _emit("ALTER TABLE " ~ table_qq ~ " ADD " ~ adapter.quote(spec.name) ~ " " ~ spec.data_type ~ " " ~ nullable ~ default_clause ~ with_values ~ ";") %}{% endcall %}
        {% endif %}
    {% endfor %}

    {# 4b - Handle existing columns: widen types, nullability, defaults #}
    {% for cname, cur in actual.items() %}
        {% set desired_spec = desired.get(cname) %}
        {% if not desired_spec %}
            {% if allow_drop_columns %}
                {% call _emit("ALTER TABLE " ~ table_qq ~ " DROP COLUMN " ~ adapter.quote(cur.name) ~ ";") %}{% endcall %}
            {% endif %}
            {% continue %}
        {% endif %}

        {# Identity mismatch => requires rebuild; just warn #}
        {% if desired_spec.identity != cur.identity %}
            {{ log("Identity setting differs on " ~ cur.name ~ ". Rebuild required; skipping.", info=True) }}
        {% endif %}

        {# Data type change #}
        {% if desired_spec.data_type and (desired_spec.data_type | lower) != (cur.data_type | lower) %}
            {# crude widen/narrow detection for varchar/nvarchar/decimal #}
            {% set can_widen = desired_spec.data_type | lower | replace('max','1000000') | int(default=0) >
                               cur.data_type | lower | replace('max','1000000') | int(default=0) %}
            {% if can_widen or allow_narrowing %}
                {% call _emit("ALTER TABLE " ~ table_qq ~ " ALTER COLUMN " ~ adapter.quote(cur.name) ~ " " ~ desired_spec.data_type ~ (" NULL" if desired_spec.nullable else " NOT NULL") ~ ";") %}{% endcall %}
            {% else %}
                {{ log("Potential narrowing on " ~ cur.name ~ " from " ~ cur.data_type ~ " to " ~ desired_spec.data_type ~ ". Skipping (set allow_narrowing=True to force).", info=True) }}
            {% endif %}
        {% endif %}

        {# Nullability change #}
        {% if desired_spec.nullable != cur.nullable %}
            {% if not desired_spec.nullable %}
                {# going to NOT NULL: backfill NULLs if a default is provided #}
                {% if desired_spec.default %}
                    {% set backfill = "UPDATE " ~ table_qq ~ " SET " ~ adapter.quote(cur.name) ~ " = (" ~ desired_spec.default ~ ") WHERE " ~ adapter.quote(cur.name) ~ " IS NULL;" %}
                    {% call _emit(backfill) %}{% endcall %}
                {% endif %}
                {% call _emit("ALTER TABLE " ~ table_qq ~ " ALTER COLUMN " ~ adapter.quote(cur.name) ~ " " ~ (desired_spec.data_type or cur.data_type) ~ " NOT NULL;") %}{% endcall %}
            {% else %}
                {% call _emit("ALTER TABLE " ~ table_qq ~ " ALTER COLUMN " ~ adapter.quote(cur.name) ~ " " ~ (desired_spec.data_type or cur.data_type) ~ " NULL;") %}{% endcall %}
            {% endif %}
        {% endif %}

        {# Default constraint alignment: drop existing if different; then add if requested #}
        {% set want_default = (desired_spec.default is not none) %}
        {% set have_default = (cur.default_definition | length) > 0 %}
        {% set default_diff = (want_default and (not have_default or (cur.default_definition | lower) != ("(" ~ desired_spec.default ~ ")" | lower))) or
                              ((not want_default) and have_default) %}

        {% if default_diff %}
            {% if have_default %}
                {% set dropdf %}
                DECLARE @df sysname;
                SELECT @df = dc.name
                FROM sys.default_constraints dc
                JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                WHERE dc.parent_object_id = OBJECT_ID(N'{{ schema_ }}.{{ identifier }}')
                  AND c.name = N'{{ cur.name }}';
                IF @df IS NOT NULL EXEC('ALTER TABLE {{ schema_ | replace("]","]]") }}.{{ identifier | replace("]","]]") }} DROP CONSTRAINT [' + @df + ']');
                {% endset %}
                {% call _emit(dropdf) %}{% endcall %}
            {% endif %}
            {% if want_default %}
                {% set default_name = 'DF_' ~ identifier ~ '_' ~ desired_spec.name %}
                {% call _emit("ALTER TABLE " ~ table_qq ~ " ADD CONSTRAINT " ~ adapter.quote(default_name) ~ " DEFAULT (" ~ desired_spec.default ~ ") FOR " ~ adapter.quote(desired_spec.name) ~ ";") %}{% endcall %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {# 5 - Execute or just print #}
    {% if stmts | length == 0 %}
        {{ log("No diffs detected for " ~ schema_ ~ "." ~ identifier, info=True) }}
    {% else %}
        {% if execute_statements %}
            {% for s in stmts %}{% do run_query(s) %}{% endfor %}
            {{ log("Applied " ~ (stmts | length) ~ " DDL statements to " ~ schema_ ~ "." ~ identifier, info=True) }}
        {% else %}
            {{ log("Planned (dry-run): " ~ (stmts | length) ~ " statements for " ~ schema_ ~ "." ~ identifier, info=True) }}
        {% endif %}
    {% endif %}

{% endmacro %}
