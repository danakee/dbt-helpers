{% macro drop_object_by_name(database, schema, object_name, object_type='auto') %}
    {# 
        Drop any database object (table, view, stored procedure, etc.) in SQL Server
        Works purely with SQL objects - no dependency on dbt models
        
        Args:
            database: Database name
            schema: Schema name
            object_name: Name of the object to drop
            object_type: Type of object - 'auto', 'table', 'view', 'procedure', 'function'
                        'auto' will query SQL Server system tables to detect object type
    #}
    
    {% if object_type == 'auto' %}
        {# Query SQL Server system tables to detect object type #}
        {% set detect_query %}
            SELECT 
                CASE type
                    WHEN 'U' THEN 'table'
                    WHEN 'V' THEN 'view'
                    WHEN 'P' THEN 'procedure'
                    WHEN 'FN' THEN 'function'
                    WHEN 'IF' THEN 'function'
                    WHEN 'TF' THEN 'function'
                    WHEN 'TR' THEN 'trigger'
                    ELSE 'unknown'
                END AS object_type
            FROM [{{ database }}].sys.objects
            WHERE name = '{{ object_name }}'
            AND schema_id = SCHEMA_ID('{{ schema }}')
        {% endset %}
        
        {% set results = run_query(detect_query) %}
        
        {% if execute and results|length > 0 %}
            {% set detected_type = results.columns[0].values()[0] %}
            {% do log("Detected object type: " ~ detected_type, info=True) %}
            
            {% set full_object_name = "[" ~ database ~ "].[" ~ schema ~ "].[" ~ object_name ~ "]" %}
            
            {% if detected_type == 'table' %}
                {% set drop_query %}
                    DROP TABLE IF EXISTS {{ full_object_name }}
                {% endset %}
            {% elif detected_type == 'view' %}
                {% set drop_query %}
                    DROP VIEW IF EXISTS {{ full_object_name }}
                {% endset %}
            {% elif detected_type == 'procedure' %}
                {% set drop_query %}
                    DROP PROCEDURE IF EXISTS {{ full_object_name }}
                {% endset %}
            {% elif detected_type == 'function' %}
                {% set drop_query %}
                    DROP FUNCTION IF EXISTS {{ full_object_name }}
                {% endset %}
            {% elif detected_type == 'trigger' %}
                {% set drop_query %}
                    DROP TRIGGER IF EXISTS {{ full_object_name }}
                {% endset %}
            {% else %}
                {% do exceptions.raise_compiler_error("Unknown or unsupported object type: " ~ detected_type) %}
            {% endif %}
            
            {% do log("Dropping " ~ detected_type ~ ": " ~ full_object_name, info=True) %}
            {% do run_query(drop_query) %}
            {% do log("Successfully dropped " ~ detected_type ~ ": " ~ full_object_name, info=True) %}
        {% else %}
            {% do log("Object does not exist: [" ~ database ~ "].[" ~ schema ~ "].[" ~ object_name ~ "]", info=True) %}
        {% endif %}
        
    {% else %}
        {# Explicit object type specified #}
        {% set full_object_name = "[" ~ database ~ "].[" ~ schema ~ "].[" ~ object_name ~ "]" %}
        
        {% if object_type|lower == 'table' %}
            {% set drop_query %}
                DROP TABLE IF EXISTS {{ full_object_name }}
            {% endset %}
        {% elif object_type|lower == 'view' %}
            {% set drop_query %}
                DROP VIEW IF EXISTS {{ full_object_name }}
            {% endset %}
        {% elif object_type|lower in ['procedure', 'proc', 'stored_procedure'] %}
            {% set drop_query %}
                DROP PROCEDURE IF EXISTS {{ full_object_name }}
            {% endset %}
        {% elif object_type|lower in ['function', 'func'] %}
            {% set drop_query %}
                DROP FUNCTION IF EXISTS {{ full_object_name }}
            {% endset %}
        {% elif object_type|lower == 'trigger' %}
            {% set drop_query %}
                DROP TRIGGER IF EXISTS {{ full_object_name }}
            {% endset %}
        {% else %}
            {% do exceptions.raise_compiler_error("Unsupported object_type: " ~ object_type ~ ". Use 'auto', 'table', 'view', 'procedure', 'function', or 'trigger'") %}
        {% endif %}
        
        {% do log("Dropping " ~ object_type ~ ": " ~ full_object_name, info=True) %}
        {% do run_query(drop_query) %}
        {% do log("Successfully dropped " ~ object_type ~ ": " ~ full_object_name, info=True) %}
    {% endif %}
{% endmacro %}