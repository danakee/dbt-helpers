{% macro drop_foreign_keys(target_table) %}

    {# Get schema and table name #}
    {%- set database_name = target_table.database -%}
    {%- set schema_name   = target_table.schema   -%}
    {%- set table_name    = target_table.identifier -%}
    {%- set full_table_name = [database_name] ~ '.' ~ '[' ~ schema_name ~ '].[' ~ table_name ~ ']' -%}
    {%- set full_table_name = full_table_name.replace('"', '') -%}
    {%- do log("INFO: Entering drop_foreign_keys for table '" ~ full_table_name ~ "'", info=True) -%}

    {% if execute %}

        {# Query to get FKs for current table #}
        {%- set get_fks_query -%}
            SELECT
                [ForeignKeyName],
                [ConditionalDropFKStatement],
                [ReferencingSchema],
                [ReferencingTable]
            FROM
                [SimulationsAnalyticsLogging].[dbo].[DataMartForeignKeyMetaData]
            WHERE
                [ReferencedSchema] = '{{ schema_name }}'
                AND [ReferencedTable] = '{{ table_name }}'
        {%- endset -%}

        {%- set results = run_query(get_fks_query) -%}

        {%- for fk in results -%}

            {%- set drop_stmt          = fk.ConditionalDropFKStatement -%}
            {%- set fk_name            = fk.ForeignKeyName -%}
            {%- set referencing_schema = fk.ReferencingSchema -%}
            {%- set referencing_table  = fk.ReferencingTable -%}

            {# Check that the CHILD (fact) table exists before attempting a drop #}
            {%- set child_rel = adapter.get_relation(
                    database=database_name,
                    schema=referencing_schema,
                    identifier=referencing_table
                ) -%}

            {%- if child_rel is none -%}
                {%- do log(
                    "DEBUG: Skipping dropping foreign key " ~ fk_name ~
                    " as table " ~ referencing_schema ~ "." ~ referencing_table ~ " does not exist",
                    info=False) -%}

                {# Optional: record a 'Skipped' row in the log table for auditability #}
                {%- set _skip_insert -%}
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
                        NEWID(),
                        '{{ fk_name }}',
                        '{{ referencing_schema }}.{{ referencing_table }}',
                        '{{ schema_name }}.{{ table_name }}',
                        'DROP',
                        'Skipped',
                        sysdatetimeoffset(),
                        sysdatetimeoffset(),
                        'Child table missing'
                    );
                {%- endset -%}
                {%- do run_query(_skip_insert) -%}
                {%- continue -%}
            {%- endif -%}

            {%- call statement('drop_fk', auto_begin=false) -%}

                DECLARE @ProcessGUID     uniqueidentifier = NEWID();
                DECLARE @FKDropStatus    nvarchar(20);
                DECLARE @FKErrorMessage  nvarchar(4000);
                SET @FKDropStatus = 'Attempted';

                -- Log the attempt
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
                    'DROP',
                    @FKDropStatus,
                    sysdatetimeoffset()
                );

                BEGIN TRY
                    -- Guarded execution: only drop if the child table & FK exist
                    IF OBJECT_ID(N'{{ '[' ~ referencing_schema ~ '].[' ~ referencing_table ~ ']' }}', 'U') IS NOT NULL
                       AND EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'{{ fk_name }}')
                    BEGIN
                        {{ drop_stmt }}
                    END
                    SET @FKDropStatus = 'Success';
                END TRY
                BEGIN CATCH
                    SET @FKErrorMessage = ERROR_MESSAGE();
                    SET @FKDropStatus = 'Failed';
                    PRINT 'Failed to drop FK {{ fk_name }} Error: ' + @FKErrorMessage;
                END CATCH

                -- Update the log with final status
                UPDATE
                    [SimulationsAnalyticsLogging].[dbo].[ForeignKeyOperationLog]
                SET
                    [OperationStatus] = @FKDropStatus,
                    [EndTime]         = sysdatetimeoffset(),
                    [ErrorMessage]    = IIF(@FKDropStatus = 'Failed', @FKErrorMessage, NULL)
                WHERE
                    [InvocationGUID]   = '{{ invocation_id }}'
                    AND [ProcessGUID]  = @ProcessGUID
                    AND [ForeignKeyName] = '{{ fk_name }}';

            {%- endcall -%}

        {%- endfor -%}

    {% endif %}

{% endmacro %}
