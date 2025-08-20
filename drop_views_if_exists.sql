{% macro drop_views_if_exists(database, views) %}
    {% if not database %}
        {{ exceptions.raise_compiler_error("drop_views_if_exists requires 'database'.") }}
    {% endif %}
    {% if not views %}
        {{ exceptions.raise_compiler_error("drop_views_if_exists requires 'views' (list of 'schema.view').") }}
    {% endif %}

    {# Accept a single string or a list #}
    {% if views is string %}
        {% set target_views = [views] %}
    {% else %}
        {% set target_views = views %}
    {% endif %}

    {% do log("Dropping views in " ~ database ~ ": " ~ (target_views | join(', ')), info=True) %}

    {% for sv in target_views %}
        {% set sv_clean = sv | trim %}
        {% set parts = sv_clean.split('.') %}

        {% if (parts | length) != 2 %}
            {% do exceptions.warn("Skipping '" ~ sv ~ "': expected 'schema.view'.") %}
            {% continue %}
        {% endif %}

        {% set schema = parts[0] | trim %}
        {% set view   = parts[1] | trim %}

        {% set rel = adapter.get_relation(database=database, schema=schema, identifier=view) %}

        {% if rel is none %}
            {% do exceptions.warn("View not found: " ~ database ~ "." ~ schema ~ "." ~ view ~ " — no action taken.") %}
        {% else %}
            {% if rel.type | lower != 'view' %}
                {% do exceptions.warn("Relation exists but type is '" ~ rel.type ~ "': " ~ rel ~ " — skipping (macro only drops views).") %}
            {% else %}
                {% do adapter.drop_relation(rel) %}
                {% do log("Dropped view: " ~ rel, info=True) %}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}
