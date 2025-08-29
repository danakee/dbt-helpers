-- macros/insert_fk_metadata.sql
{% macro insert_fk_metadata(target_table=none, referencing_table=none) %}
  
    {%- if target_table -%}
        {%- set metadata_relation = target_table -%}
    {%- else -%}
        {%- set metadata_relation = api.Relation.create(
            database=var('fk_metadata_database', 'SimulationsAnalyticsLogging'),
            schema=var('fk_metadata_schema', 'dbo'), 
            identifier=var('fk_metadata_table', 'DataMartForeignKeyMetaData')
        ) -%}
    {%- endif -%}
    
    {%- set view_relation = api.Relation.create(
        database=var('fk_view_database', 'SimulationsAnalyticsLogging'),
        schema=var('fk_view_schema', 'dbo'), 
        identifier=var('fk_view_table', 'vw_CurrentDataMartForeignKeys')
    ) -%}
  
    {%- set insert_query -%}
    INSERT INTO {{ metadata_relation }} (
         [ReferencingSchema]
        ,[ReferencingTable]
        ,[ReferencedSchema]
        ,[ReferencedTable]
        ,[ForeignKeyName]
        ,[DropFKStatement]
        ,[CreateFKStatement]
        ,[ConditionalDropFKStatement]
        ,[ConditionalCreateFKStatement]
        ,[IsFKEnabled]
    )
    SELECT 
         [ReferencingSchema]
        ,[ReferencingTable]
        ,[ReferencedSchema]
        ,[ReferencedTable]
        ,[ForeignKeyName]
        ,[DropFKStatement]
        ,[CreateFKStatement]
        ,[ConditionalDropFKStatement]
        ,[ConditionalCreateFKStatement]
        ,[IsFKEnabled]
    FROM {{ view_relation }} AS [v]
    WHERE 1=1
        AND NOT EXISTS (
            SELECT * 
            FROM {{ metadata_relation }} AS [m] 
            WHERE 
                [m].[ForeignKeyName] = [v].[ForeignKeyName]
                AND [m].[ReferencingSchema] = [v].[ReferencingSchema]
                AND [m].[ReferencingTable] = [v].[ReferencingTable]
        )
        {% if referencing_table %}
        {%- set safe_referencing_table = referencing_table | replace("'", "''") -%}
        AND [ReferencingTable] = '{{ safe_referencing_table }}'
        {% endif %}
    {%- endset -%}

    {{ return(insert_query) }}

{% endmacro %}


-- Alternative macro for executing the insert directly with statement block
{% macro execute_fk_metadata_insert(target_table=none, referencing_table=none) %}
  
    {% if execute %}
        {%- set insert_sql = insert_fk_metadata(target_table, referencing_table) -%}
        
        {%- do log("Inserting foreign key metadata...") -%}
        {%- do log("Target table: " ~ (target_table or "default")) -%}
        {%- if referencing_table -%}
            {%- do log("Referencing table filter: " ~ referencing_table) -%}
        {%- endif -%}
        
        {%- call statement('insert_fk_metadata', fetch_result=True) -%}
          {{ insert_sql }}
        {%- endcall -%}
        
        {%- set result = load_result('insert_fk_metadata') -%}
        {%- do log("Foreign key metadata insert completed. Rows affected: " ~ result.get('response', {}).get('rows_affected', 'unknown')) -%}

    {% endif %}

{% endmacro %}


-- Macro with exclusion capability for more control
{% macro insert_fk_metadata_selective(target_table=none, referencing_table=none, exclude_tables=[], exclude_fks=[]) %}
  
    {%- if target_table -%}
        {%- set metadata_relation = target_table -%}
    {%- else -%}
        {%- set metadata_relation = api.Relation.create(
            database=var('fk_metadata_database', 'SimulationsAnalyticsLogging'),
            schema=var('fk_metadata_schema', 'dbo'), 
            identifier=var('fk_metadata_table', 'DataMartForeignKeyMetaData')
        ) -%}
    {%- endif -%}
    
    {%- set view_relation = api.Relation.create(
        database=var('fk_view_database', 'SimulationsAnalyticsLogging'),
        schema=var('fk_view_schema', 'dbo'), 
        identifier=var('fk_view_table', 'vw_CurrentDataMartForeignKeys')
    ) -%}
    
    {%- set safe_exclude_tables = [] -%}
    {%- for table in exclude_tables -%}
        {%- set safe_exclude_tables = safe_exclude_tables.append("'" + (table | replace("'", "''")) + "'") -%}
    {%- endfor -%}
    
    {%- set safe_exclude_fks = [] -%}
    {%- for fk in exclude_fks -%}
        {%- set safe_exclude_fks = safe_exclude_fks.append("'" + (fk | replace("'", "''")) + "'") -%}
    {%- endfor -%}
    
    {%- set insert_query -%}
    INSERT INTO {{ metadata_relation }} (
         [ReferencingSchema]
        ,[ReferencingTable]
        ,[ReferencedSchema]
        ,[ReferencedTable]
        ,[ForeignKeyName]
        ,[DropFKStatement]
        ,[CreateFKStatement]
        ,[ConditionalDropFKStatement]
        ,[ConditionalCreateFKStatement]
        ,[IsFKEnabled]
    )
    SELECT 
         [ReferencingSchema]
        ,[ReferencingTable]
        ,[ReferencedSchema]
        ,[ReferencedTable]
        ,[ForeignKeyName]
        ,[DropFKStatement]
        ,[CreateFKStatement]
        ,[ConditionalDropFKStatement]
        ,[ConditionalCreateFKStatement]
        ,[IsFKEnabled]
    FROM 
        {{ view_relation }} AS [v]
    WHERE
        NOT EXISTS (
            SELECT * 
            FROM {{ metadata_relation }} AS [m] 
            WHERE 
                [m].[ForeignKeyName] = [v].[ForeignKeyName]
                AND [m].[ReferencingSchema] = [v].[ReferencingSchema]
                AND [m].[ReferencingTable] = [v].[ReferencingTable]
        )
        {% if referencing_table %}
        {%- set safe_referencing_table = referencing_table | replace("'", "''") -%}
        AND [ReferencingTable] = '{{ safe_referencing_table }}'
        {% endif %}
        {% if safe_exclude_tables %}
        AND [ReferencingTable] NOT IN ({{ safe_exclude_tables | join(',') }})
        {% endif %}
        {% if safe_exclude_fks %}
        AND [ForeignKeyName] NOT IN ({{ safe_exclude_fks | join(',') }})
        {% endif %}
    {%- endset -%}
  
    {{ return(insert_query) }}

{% endmacro %}


-- Preview macro to see what WOULD be inserted (dry run)
{% macro preview_fk_metadata_changes(referencing_table=none, exclude_tables=[], exclude_fks=[]) %}
  
    {%- set metadata_relation = api.Relation.create(
        database=var('fk_metadata_database', 'SimulationsAnalyticsLogging'),
        schema=var('fk_metadata_schema', 'dbo'), 
        identifier=var('fk_metadata_table', 'DataMartForeignKeyMetaData')
    ) -%}
    
    {%- set view_relation = api.Relation.create(
        database=var('fk_view_database', 'SimulationsAnalyticsLogging'),
        schema=var('fk_view_schema', 'dbo'), 
        identifier=var('fk_view_table', 'vw_CurrentDataMartForeignKeys')
    ) -%}
    
    {%- set safe_exclude_tables = [] -%}
    {%- for table in exclude_tables -%}
        {%- set safe_exclude_tables = safe_exclude_tables.append("'" + (table | replace("'", "''")) + "'") -%}
    {%- endfor -%}
    
    {%- set safe_exclude_fks = [] -%}
    {%- for fk in exclude_fks -%}
        {%- set safe_exclude_fks = safe_exclude_fks.append("'" + (fk | replace("'", "''")) + "'") -%}
    {%- endfor -%}
    
    SELECT 
         [ReferencingSchema]
        ,[ReferencingTable]
        ,[ReferencedSchema]
        ,[ReferencedTable]
        ,[ForeignKeyName]
        ,'WOULD BE INSERTED' AS [Action]
    FROM 
        {{ view_relation }} AS [v]
    WHERE 
        NOT EXISTS (
            SELECT 
                * 
            FROM 
                {{ metadata_relation }} AS [m] 
            WHERE 
                [m].[ForeignKeyName] = [v].[ForeignKeyName]
                AND [m].[ReferencingSchema] = [v].[ReferencingSchema]
                AND [m].[ReferencingTable] = [v].[ReferencingTable]
        )
        {% if referencing_table %}
        {%- set safe_referencing_table = referencing_table | replace("'", "''") -%}
        AND [ReferencingTable] = '{{ safe_referencing_table }}'
        {% endif %}
        {% if safe_exclude_tables %}
        AND [ReferencingTable] NOT IN ({{ safe_exclude_tables | join(',') }})
        {% endif %}
        {% if safe_exclude_fks %}
        AND [ForeignKeyName] NOT IN ({{ safe_exclude_fks | join(',') }})
        {% endif %}
    ORDER BY 
         [ReferencingTable]
        ,[ForeignKeyName]
    
{% endmacro %}


-- Macro specifically for fact tables (following your naming convention)
{% macro insert_fact_table_fk_metadata(fact_table_name, target_table=none) %}
  
  {%- if not fact_table_name.startswith('Fact') -%}
    {%- set qualified_fact_table = 'Fact' ~ fact_table_name -%}
  {%- else -%}
    {%- set qualified_fact_table = fact_table_name -%}
  {%- endif -%}
  
  {{ insert_fk_metadata(target_table, qualified_fact_table) }}
  
{% endmacro %}


-- Macro to get FK metadata as a query (useful for testing/validation)
{% macro get_new_fk_metadata(referencing_table=none) %}
  
    {%- set metadata_relation = api.Relation.create(
        database=var('fk_metadata_database', 'SimulationsAnalyticsLogging'),
        schema=var('fk_metadata_schema', 'dbo'), 
        identifier=var('fk_metadata_table', 'DataMartForeignKeyMetaData')
    ) -%}
    
    {%- set view_relation = api.Relation.create(
        database=var('fk_view_database', 'SimulationsAnalyticsLogging'),
        schema=var('fk_view_schema', 'dbo'), 
        identifier=var('fk_view_table', 'vw_CurrentDataMartForeignKeys')
    ) -%}
    
    SELECT 
         [ReferencingSchema]
        ,[ReferencingTable]
        ,[ReferencedSchema]
        ,[ReferencedTable]
        ,[ForeignKeyName]
        ,[DropFKStatement]
        ,[CreateFKStatement]
        ,[ConditionalDropFKStatement]
        ,[ConditionalCreateFKStatement]
        ,[IsFKEnabled]
    FROM 
        {{ view_relation }} AS [v]
    WHERE 
        NOT EXISTS (
            SELECT * 
            FROM {{ metadata_relation }} AS [m] 
            WHERE [m].[ForeignKeyName] = [v].[ForeignKeyName]
                AND [m].[ReferencingSchema] = [v].[ReferencingSchema]
                AND [m].[ReferencingTable] = [v].[ReferencingTable]
        )
        {% if referencing_table %}
        {%- set safe_referencing_table = referencing_table | replace("'", "''") -%}
        AND [ReferencingTable] = '{{ safe_referencing_table }}'
        {% endif %}
  
{% endmacro %}
