{# 
    sqlserver_identifiers.sql
    --------------------------
    Utility macros for safely handling SQL Server identifiers.
    Wraps schema, table, column, and alias names in square brackets and escapes internal brackets.
#}

{% macro bracket_ident(name) %}
    {# Safely wraps an identifier in [] and escapes internal ] characters. #}
    {% if not name %}
        {{ return('') }}
    {% endif %}
    {% if name.startswith('[') and name.endswith(']') %}
        {{ return(name) }}
    {% else %}
        {{ return('[' ~ (name | replace(']', ']]')) ~ ']') }}
    {% endif %}
{% endmacro %}

{% macro quote_identifier(schema_name, table_name) %}
    {# Example usage: quote_identifier('dbo', 'DimUser') → [dbo].[DimUser] #}
    {% if schema_name and table_name %}
        {{ return(bracket_ident(schema_name) ~ '.' ~ bracket_ident(table_name)) }}
    {% elif table_name %}
        {{ return(bracket_ident(table_name)) }}
    {% else %}
        {{ return('') }}
    {% endif %}
{% endmacro %}

{% macro fully_qualified_name(database, schema, table) %}
    {# Returns a fully qualified table name, skipping any missing levels. #}
    {% set parts = [] %}
    {% if database %} {% do parts.append(bracket_ident(database)) %} {% endif %}
    {% if schema %} {% do parts.append(bracket_ident(schema)) %} {% endif %}
    {% if table %} {% do parts.append(bracket_ident(table)) %} {% endif %}
    {{ return(parts | join('.')) }}
{% endmacro %}
