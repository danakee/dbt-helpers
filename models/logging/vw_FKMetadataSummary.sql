{{ config(
    materialized='view',
    alias='vw_FKMetadataSummary'
) }}

WITH [MetaDataStats] AS (
    SELECT 
         [ReferencingTable]
        ,COUNT(*) AS [ForeignKeyCount]
        ,SUM(CASE WHEN [IsFKEnabled] = 1 THEN 1 ELSE 0 END) AS [EnabledFKCount]
        ,SUM(CASE WHEN [IsFKEnabled] = 0 THEN 1 ELSE 0 END) AS [DisabledFKCount]
    FROM 
        {{ source('logging', 'DataMartForeignKeyMetaData') }}
    GROUP BY 
        [ReferencingTable]
),

[CurrentFKs] AS (
SELECT DISTINCT
    [ReferencingTable]
FROM 
    {{ source('logging', 'vw_CurrentDataMartForeignKeys') }}
)

SELECT 
    COALESCE([m].[ReferencingTable], [c].[ReferencingTable]) AS [ReferencingTable],
    COALESCE([m].[ForeignKeyCount], 0) AS [ForeignKeyCount],
    COALESCE([m].[EnabledFKCount], 0) AS [EnabledFKCount],
    COALESCE([m].[DisabledFKCount], 0) AS [DisabledFKCount],
    CASE WHEN [m].[ReferencingTable] IS NOT NULL THEN 1 ELSE 0 END AS [HasMetadata],
    CASE 
        WHEN [m].[ReferencingTable] IS NULL AND [c].[ReferencingTable] IS NOT NULL 
        THEN 'Missing Metadata'
        WHEN [m].[ReferencingTable] IS NOT NULL AND [c].[ReferencingTable] IS NULL 
        THEN 'Stale Metadata'
        ELSE 'OK'
    END AS [Status]
FROM 
    [MetaDataStats] AS [m]
    FULL OUTER JOIN [CurrentFKs] AS [c]
        ON [m].[ReferencingTable] = [c].[ReferencingTable];
