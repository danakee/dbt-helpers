{{ config(
    materialized='view',
    alias='vw_FKHealthCheck'
) }}

SELECT 
    [v].[ReferencingSchema],
    [v].[ReferencingTable],
    [v].[ForeignKeyName],
    'Missing FK Metadata' AS [Issue],
    'FK exists in database but not in metadata table' AS [Description]
FROM 
    {{ source('logging', 'vw_CurrentDataMartForeignKeys') }} AS [v]
    LEFT JOIN {{ source('logging', 'DataMartForeignKeyMetaData') }} AS [m]
        ON [v].[ForeignKeyName] = [m].[ForeignKeyName]
        AND [v].[ReferencingSchema] = [m].[ReferencingSchema]
        AND [v].[ReferencingTable] = [m].[ReferencingTable]
WHERE 
    [m].[ForeignKeyName] IS NULL

UNION ALL

SELECT 
    [m].[ReferencingSchema],
    [m].[ReferencingTable],
    [m].[ForeignKeyName],
    'Stale FK Metadata' AS [Issue],
    'FK in metadata table but no longer exists in database' AS [Description]
FROM 
    {{ source('logging', 'DataMartForeignKeyMetaData') }} AS [m]
    LEFT JOIN {{ source('logging', 'vw_CurrentDataMartForeignKeys') }} AS [v]
        ON  [m].[ForeignKeyName] = [v].[ForeignKeyName]
        AND [m].[ReferencingSchema] = [v].[ReferencingSchema]
        AND [m].[ReferencingTable] = [v].[ReferencingTable]
WHERE 
    [v].[ForeignKeyName] IS NULL;
