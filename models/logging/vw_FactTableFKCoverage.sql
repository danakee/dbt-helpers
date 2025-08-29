{{ config(
    materialized='view',
    alias='vw_FactTableFKCoverage'
) }}

-- Monitor which fact tables have FK metadata coverage
WITH [FactTables] AS (
SELECT DISTINCT
    [ReferencingTable]
FROM 
    {{ source('logging', 'vw_CurrentDataMartForeignKeys') }}
WHERE 
    [ReferencingTable] LIKE 'Fact%'
),

[MetadataCoverage] AS (
SELECT DISTINCT
    [ReferencingTable]
FROM 
    {{ source('logging', 'DataMartForeignKeyMetaData') }}
WHERE 
    [ReferencingTable] LIKE 'Fact%'
)

SELECT 
     [f].[ReferencingTable]
    ,CASE WHEN [m].[ReferencingTable] IS NOT NULL THEN 'Covered' ELSE 'Not Covered' END AS [MetadataCoverage]
    ,CASE WHEN [m].[ReferencingTable] IS NULL THEN 1 ELSE 0 END AS [NeedsMetadata]
FROM 
    [FactTables] AS [f]
    LEFT JOIN [MetadataCoverage] AS [m]
        ON [f].[ReferencingTable] = [m].[ReferencingTable]
ORDER BY 
     [MetadataCoverage]
    ,[f].[ReferencingTable];
