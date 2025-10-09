{% macro drop_stage_tables() %}
    {# 
        Drops all tables in [SimulationsAnalyticsStage].[dbo] that start with 'Stage'
        No parameters needed - just run: dbt run-operation drop_stage_tables
    #}
    
    {% do log("=== Starting Stage Tables Cleanup ===", info=True) %}
    {% do log("Database: SimulationsAnalyticsStage", info=True) %}
    {% do log("Schema: dbo", info=True) %}
    {% do log("Pattern: Tables starting with 'Stage'", info=True) %}
    {% do log("", info=True) %}
    
    {# Query to find all tables starting with 'Stage' #}
    {% set find_tables_query %}
        SELECT 
            [name] AS [TableName]
        FROM 
            [SimulationsAnalyticsStage].[sys].[tables]
        WHERE 
            [schema_id] = SCHEMA_ID('dbo')
            AND [name] LIKE 'Stage%'
        ORDER BY 
            [name]
    {% endset %}
    
    {% set results = run_query(find_tables_query) %}
    
    {% if execute %}
        {% if results|length > 0 %}
            {% do log("Found " ~ results|length ~ " table(s) to drop:", info=True) %}
            
            {# Loop through each table and drop it #}
            {% for row in results %}
                {% set table_name = row[0] %}
                {% do log("  - " ~ table_name, info=True) %}
                
                {% set drop_query %}
                    DROP TABLE IF EXISTS [SimulationsAnalyticsStage].[dbo].[{{ table_name }}]
                {% endset %}
                
                {% do run_query(drop_query) %}
                {% do log("    ✓ Dropped successfully", info=True) %}
            {% endfor %}
            
            {% do log("", info=True) %}
            {% do log("=== Cleanup Complete! Dropped " ~ results|length ~ " table(s) ===", info=True) %}
        {% else %}
            {% do log("No tables found matching pattern 'Stage%'", info=True) %}
            {% do log("=== Cleanup Complete! Nothing to drop ===", info=True) %}
        {% endif %}
    {% endif %}
{% endmacro %}