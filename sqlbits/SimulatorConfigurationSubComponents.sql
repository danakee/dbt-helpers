-- 1) Base joins + simulator filters (exclude non-active sims here)
WITH [BasePaths] AS (
SELECT
     [s].[pkey]                 AS [SimulatorPKey]
    ,ISNULL([h1].[fk_tail], -1) AS [ConfigurationPKey]
    ,ISNULL([h2].[fk_tail], -1) AS [ComponentPKey]
    ,ISNULL([h3].[fk_tail], -1) AS [SubComponentPKey]
    -- hvr_change_time: use GREATEST in 2022, VALUES/MAX otherwise (see below)
    ,GREATEST(
        [s].[hvr_change_time],
        [h1].[hvr_change_time],
        [h2].[hvr_change_time],
        [h3].[hvr_change_time]
    ) AS [hvr_change_time]
FROM 
    [Sim2].[dbo].[tblSim] AS [s]
    LEFT OUTER JOIN [Sim2].[dbo].[tblObjectHier] AS [h1] -- sim to config
        ON  [h1].[fk_classlink] = 2
        AND [s].[pkey] = [h1].[fk_head]
    LEFT OUTER JOIN [Sim2].[dbo].[tblObjectHier] AS [h2] -- config to component
        ON  [h2].[fk_classlink] = 3
        AND [h1].[fk_tail] = [h2].[fk_head]
        AND [h1].[pkey] = [h2].[fk_parent]
    LEFT OUTER JOIN [Sim2].[dbo].[tblObjectHier] AS [h3] -- component to subcomponent
        ON  [h3].[fk_classlink] = 4
        AND [h2].[fk_tail] = [h3].[fk_head]
        AND [h2].[pkey] = [h3].[fk_parent]
WHERE
    [s].[latest] = 1
    AND [s].[fk_status] NOT IN (2,3,4,5)  -- 2:Hibernation, 3:Decommissioned, 4:Scrapped, 5:Cancelled
    AND [s].[name] <> 'Quality System'    -- eliminate Quality System
),

-- 2) De-dup the path rows before window calcs
[DistinctPaths] AS (
 SELECT DISTINCT
     [SimulatorPKey]
    ,[ConfigurationPKey]
    ,[ComponentPKey]
    ,[SubComponentPKey]
    ,[hvr_change_time]
FROM 
    [BasePaths]
),

-- 3) Add measures + flags on the deduped set
[SimConfigSubComponent] AS
(
SELECT
     [p].[SimulatorPKey]
    ,[p].[ConfigurationPKey]
    ,[p].[ComponentPKey]
    ,[p].[SubComponentPKey]
    -- Measures (fully additive)
    ,CAST(1 AS INT) AS [RelationshipCount]
    ,CASE WHEN [p].[ComponentPKey]    <> -1
            AND [p].[SubComponentPKey] = -1 THEN 1 ELSE 0 END AS [ComponentCount]
    ,CASE WHEN [p].[SubComponentPKey] <> -1 THEN 1 ELSE 0 END AS [SubComponentCount]
    -- Distinct-count helper flags (first row per group)
    ,ROW_NUMBER() OVER (
        PARTITION BY 
             [p].[SimulatorPKey]
            ,[p].[ConfigurationPKey]
        ORDER BY     
             [p].[ComponentPKey]
            ,[p].[SubComponentPKey]
    ) AS [ConfigurationRowNumber]
    ,ROW_NUMBER() OVER (
        PARTITION BY 
             [p].[SimulatorPKey]
            ,[p].[ConfigurationPKey]
            ,[p].[ComponentPKey]
        ORDER BY     
            [p].[SubComponentPKey]
    ) AS [ComponentRowNumber]
    -- Active after WHERE pre-filter
    ,CAST(1 AS BIT) AS [IsActive]
    -- Composite natural-key hash (fixed width)
    ,CONVERT(BINARY(32), HASHBYTES(
        'SHA2_256',
        CONCAT(
            CAST([p].[SimulatorPKey]     AS VARCHAR(20)), '|',
            CAST([p].[ConfigurationPKey] AS VARCHAR(20)), '|',
            CAST([p].[ComponentPKey]     AS VARCHAR(20)), '|',
            CAST([p].[SubComponentPKey]  AS VARCHAR(20))
        )
    )) AS [NaturalKeyHash]
    ,[p].[hvr_change_time]
FROM 
    [DistinctPaths] AS [p]
),

-- 4) Final projection (your existing alias)
[Final] AS (
SELECT
     [s].[SimulatorPKey]
    ,[s].[ConfigurationPKey]
    ,[s].[ComponentPKey]
    ,[s].[SubComponentPKey]
    ,[s].[RelationshipCount]
    ,IIF([s].[ConfigurationRowNumber] = 1, 1, 0) AS [ConfigurationCount]
    ,[s].[ComponentCount]
    ,IIF([s].[ComponentRowNumber] = 1, 1, 0) AS [ComponentDistinctCount]
    ,[s].[SubComponentCount]
    ,[s].[ConfigurationRowNumber]
    ,[s].[ComponentRowNumber]
    ,[s].[IsActive]
    ,[s].[NaturalKeyHash]
    ,[s].[hvr_change_time]
FROM 
    [SimConfigSubComponent] AS [s]
)
SELECT
    *
FROM 
    [Final]
ORDER BY
     [SimulatorPKey]
    ,[ConfigurationPKey]
    ,[ComponentPKey]
    ,[SubComponentPKey];
