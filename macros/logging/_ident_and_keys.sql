{# 
    _ident_and_keys.sql
    --------------------
    Helper macros that build on sqlserver_identifiers.sql
    Used for rendering comma-separated column lists and composite key predicates.
#}

{% macro _ensure_list(value) %}
    {# Converts a single value or None to a list for consistent iteration. #}
    {% if value is string %}
        {{ return([value]) }}
    {% elif value is not none %}
        {{ return(value) }}
    {% else %}
        {{ return([]) }}
    {% endif %}
{% endmacro %}

{% macro render_col_list(cols, alias=None) -%}
    {# Returns comma-separated list like [alias].[col1], [alias].[col2] #}
    {%- set cols = _ensure_list(cols) -%}
    {%- set a = bracket_ident(alias) if alias else '' -%}
    {%- for c in cols -%}
        {{ (a ~ '.' if alias else '') ~ bracket_ident(c) }}{{ ', ' if not loop.last }}
    {%- endfor -%}
{%- endmacro %}

{% macro render_key_predicate(keys, left_alias, right_alias) -%}
    {# Builds AND-separated key match predicate for composite keys. #}
    {%- set keys = _ensure_list(keys) -%}
    {%- set la = bracket_ident(left_alias) -%}
    {%- set ra = bracket_ident(right_alias) -%}
    {%- for k in keys -%}
        {{ la }}.{{ bracket_ident(k) }} = {{ ra }}.{{ bracket_ident(k) }}{% if not loop.last %} AND {% endif %}
    {%- endfor -%}
{%- endmacro %}
