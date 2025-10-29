{% macro create_missing_foreign_keys(target_table) %}

    {# Get schema and table name #}
    {%- set database_name = target_table.database -%}
    {%- set schema_name   = target_table.schema   -%}         {# parent (referenced) schema #}
    {%- set table_name    = target_table.identifier -%}       {# parent (referenced) table  #}
    {%- set full_table_name = [database_name] ~ '.' ~ '[' ~ schema_name ~ '].[' ~ table_name ~ ']' -%}
    {%- set full_table_name = full_table_name.replace('"', '') -%}
    {%- do log("INFO: Entering create_missing_foreign_keys for table '" ~ full_table_name ~ "'", info=True) -%}

    {% if execute %}

        {# Query to get FKs that should exist for this table but currently do not #}
        {%- set get_fks_query -%}
            SELECT
                [m].[ForeignKeyName],
                [m].[ConditionalCreateFKStatement],
                [m].[ReferencingSchema],
                [m].[ReferencingTable]
            FROM
                [SimulationsAnalyticsLogging].[dbo].[DataMartForeignKeyMetaData] AS [m]
                LEFT OUTER JOIN [SimulationsAnalytics].[sys].[foreign_keys] AS [fk]
                    ON [fk].[name] = [m].[ForeignKeyName]
            WHERE
                [m].[ReferencedSchema] = '{{ schema_name }}'
                AND [m].[ReferencedTable]  = '{{ table_name }}'
                AND [fk].[name] IS NULL   -- only those not present yet
        {%- endset -%}

        {%- set results = run_query(get_fks_query) -%}

        {%- for row in results.rows -%}

            {%- set create_stmt        = row['ConditionalCreateFKStatement'] -%}
            {%- set fk_name            = row['ForeignKeyName'] -%}
            {%- set referencing_schema = row['ReferencingSchema'] -%}   {# child schema #}
            {%- set referencing_table  = row['ReferencingTable'] -%}    {# child table  #}

            {# Jinja-side existence checks for both sides #}
            {%- set child_rel = adapter.get_relation(
                    database=database_name,
                    schema=referencing_schema,
                    identifier=referencing_table
                ) -%}
            {%- set parent_rel = adapter.get_relation(
                    database=database_name,
                    schema=schema_name,
                    identifier=table_name
                ) -%}

            {%- if child_rel is none or parent_rel is none -%}
                {%- set missing_side = 'child' if child_rel is none else 'parent' -%}
                {%- do log(
                    "DEBUG: Skipping CREATE FK " ~ fk_name ~
                    " because " ~ missing_side ~ " table is missing (" ~
                    referencing_schema ~ "." ~ referencing_table ~ " -> " ~
                    schema_name ~ "." ~ table_name ~ ")", info=False) -%}

                {# Single 'Skipped' row for auditability #}
                {%- set _skip_block -%}
                    DECLARE @ProcessGUID uniqueidentifier = NEWID();

                    INSERT INTO [SimulationsAnalyticsLogging].[dbo].[ForeignKeyOperationLog]
                    (
                        [InvocationGUID],
                        [ProcessGUID],
                        [ForeignKeyName],
                        [SourceObject],
                        [TargetObject],
                        [Operation],
                        [OperationStatus],
                        [StartTime],
                        [EndTime],
                        [ErrorMessage]
                    )
                    VALUES
                    (
                        '{{ invocation_id }}',
                        @ProcessGUID,
                        '{{ fk_name }}',
                        '{{ referencing_schema }}.{{ referencing_table }}',
                        '{{ schema_name }}.{{ table_name }}',
                        'CREATE',
                        'Skipped',
                        sysdatetimeoffset(),
                        sysdatetimeoffset(),
                        'Missing ' + N'{{ missing_side }}' + ' table; no create executed'
                    );
                {%- endset -%}
                {%- do run_query(_skip_block) -%}
                {%- continue -%}
            {%- endif -%}

            {%- call statement('create_fk', auto_begin=false) -%}

                DECLARE @ProcessGUID       uniqueidentifier = NEWID();
                DECLARE @FKCreateStatus    nvarchar(20);
                DECLARE @CreateFKErrorMessage nvarchar(4000);

                -- Initial row (NOT NULL and allowed by CHECK): 'Attempted'
                INSERT INTO [SimulationsAnalyticsLogging].[dbo].[ForeignKeyOperationLog]
                (
                    [InvocationGUID],
                    [ProcessGUID],
                    [ForeignKeyName],
                    [SourceObject],
                    [TargetObject],
                    [Operation],
                    [OperationStatus],
                    [StartTime]
                )
                VALUES
                (
                    '{{ invocation_id }}',
                    @ProcessGUID,
                    '{{ fk_name }}',
                    '{{ referencing_schema }}.{{ referencing_table }}',
                    '{{ schema_name }}.{{ table_name }}',
                    'CREATE',
                    'Attempted',
                    sysdatetimeoffset()
                );

                BEGIN TRY
                    -- SQL-side guards: both tables exist and FK still absent
                    IF  OBJECT_ID(N'{{ '[' ~ referencing_schema ~ '].[' ~ referencing_table ~ ']' }}', 'U') IS NOT NULL
                    AND OBJECT_ID(N'{{ '[' ~ schema_name ~ '].[' ~ table_name ~ ']' }}', 'U') IS NOT NULL
                    AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'{{ fk_name }}')
                    BEGIN
                        {{ create_stmt }}
                    END
                    SET @FKCreateStatus = 'Success';
                END TRY
                BEGIN CATCH
                    SET @FKCreateStatus = 'Failed';
                    SET @CreateFKErrorMessage = ERROR_MESSAGE();
                    PRINT 'Failed to create FK {{ fk_name }}: ' + @CreateFKErrorMessage;
                END CATCH

                -- Final status
                UPDATE  [SimulationsAnalyticsLogging].[dbo].[ForeignKeyOperationLog]
                SET     [OperationStatus] = @FKCreateStatus,
                        [EndTime]        = sysdatetimeoffset(),
                        [ErrorMessage]   = IIF(@FKCreateStatus = 'Failed', @CreateFKErrorMessage, NULL)
                WHERE   [InvocationGUID]   = '{{ invocation_id }}'
                  AND   [ProcessGUID]     = @ProcessGUID
                  AND   [ForeignKeyName]  = '{{ fk_name }}';

            {%- endcall -%}

        {%- endfor -%}

    {% endif %}

{% endmacro %}
