WITH [AssigneeDetail] AS (
SELECT
    [fk_issue]      AS [IssuePKey]
   ,[prim_key]      AS [NodeDataPKey]
   ,[fk_assignid]   AS [IssueAssigneePKey]
   ,[fk_assigntype] AS [IssueAssigneeTypePKey]
   ,[create_dt]     AS [NodeCreateDateTime]
   ,ROW_NUMBER() OVER (
        PARTITION BY 
            [fk_issue]
        ORDER BY 
            [create_dt], [prim_key]
   ) AS [IssueNodeDateSeqNum]
   ,LAG([fk_assignid])   OVER (PARTITION BY [fk_issue] ORDER BY [create_dt], [prim_key]) AS [prev_assignid]
   ,LAG([fk_assigntype]) OVER (PARTITION BY [fk_issue] ORDER BY [create_dt], [prim_key]) AS [prev_assigntype]
   ,[hvr_change_time]
FROM 
    [Fsi_Issues2].[dbo].[tblnodedata]
WHERE 
    [create_dt] >= '2007-01-01'
    AND [latest] = 1
),
[Normalized] AS (
SELECT
     *
    ,CASE
        WHEN NOT EXISTS (SELECT [IssueAssigneePKey]     INTERSECT SELECT [prev_assignid])
        OR NOT EXISTS (SELECT [IssueAssigneeTypePKey] INTERSECT SELECT [prev_assigntype])
        THEN 1 ELSE 0
    END AS [ChangeFlag]
FROM 
    [AssigneeDetail]
),

[Runs] AS (
SELECT
     *
    ,SUM([ChangeFlag]) OVER (
        PARTITION BY 
            [IssuePKey]
        ORDER BY 
            [NodeCreateDateTime], [NodeDataPKey]
        ROWS UNBOUNDED PRECEDING
    ) AS [RunId]
FROM 
    [Normalized]
),

[RunEdges] AS (
SELECT
     [r].[IssuePKey]
    ,[r].[IssueAssigneePKey]
    ,[r].[IssueAssigneeTypePKey]
    ,[r].[NodeCreateDateTime]
    ,[r].[NodeDataPKey]
    ,[r].[hvr_change_time]
    ,[r].[RunId]
    ,ROW_NUMBER() OVER (
        PARTITION BY 
            r.[IssuePKey], r.[RunId]
        ORDER BY 
            r.[NodeCreateDateTime], r.[NodeDataPKey]
    ) AS [rn_in_run]
    ,MIN(r.[NodeCreateDateTime]) OVER (
        PARTITION BY 
            r.[IssuePKey], r.[RunId]
    ) AS [RunStartDateTime]
    ,MAX(r.[NodeCreateDateTime]) OVER (
        PARTITION BY 
            r.[IssuePKey], r.[RunId]
    ) AS [RunLastSeenDateTime]
    ,MAX(r.[hvr_change_time]) OVER (
        PARTITION BY 
            r.[IssuePKey], r.[RunId]
    ) AS [RunChangeTime]  -- single watermark for the entire run
FROM 
    [Runs] AS r
),

[Collapsed] AS (
SELECT
    [e].[IssuePKey]
   ,[e].[IssueAssigneePKey]
   ,[e].[IssueAssigneeTypePKey]
   ,[e].[RunStartDateTime]
   ,[e].[RunLastSeenDateTime]
   ,[e].[RunChangeTime]
FROM 
    [RunEdges] AS e
WHERE 
    e.[rn_in_run] = 1
),

[Final] AS (
SELECT
     [c].[IssuePKey]
    ,[c].[IssueAssigneePKey]
    ,[c].[IssueAssigneeTypePKey]
    ,[c].[RunStartDateTime]
    ,[c].[RunLastSeenDateTime]
    ,[c].[RunChangeTime]
    ,LEAD(c.[RunStartDateTime]) OVER (
        PARTITION BY 
            c.[IssuePKey]
        ORDER BY 
            c.[RunStartDateTime]
    ) AS [RunEndDateTime]  -- next run's start = exclusive end
FROM 
    [Collapsed] AS c
)
SELECT
     [IssuePKey]                                                           AS [IssuePKey]
    ,[IssueAssigneePKey]                                                   AS [IssueAssigneePKey]
    ,[IssueAssigneeTypePKey]                                               AS [IssueAssigneeTypePKey]
    ,[RunStartDateTime]                                                    AS [IssueAssigneeStartDateTime]
    ,[RunEndDateTime]                                                      AS [IssueAssigneeEndDateTime]          -- NULL = still current/open
    ,[RunLastSeenDateTime]                                                 AS [IssueAssigneeLastSeenDateTime]
    ,[RunChangeTime]                                                       AS [hvr_change_time]                    -- single watermark per collapsed row
    ,DATEDIFF(MINUTE, [RunStartDateTime], COALESCE([RunEndDateTime], [RunLastSeenDateTime])) AS [IssueAssigneeDurationMinutes]
    ,CAST(1.0 * DATEDIFF(MINUTE, [RunStartDateTime], COALESCE([RunEndDateTime], [RunLastSeenDateTime])) / 60.0  AS DECIMAL(18, 2)) AS [IssueAssigneeDurationHours]
    ,CAST(1.0 * DATEDIFF(MINUTE, [RunStartDateTime], COALESCE([RunEndDateTime], [RunLastSeenDateTime])) / 1440.0 AS DECIMAL(18, 2)) AS [IssueAssigneeDurationDays]
FROM 
    [Final]
ORDER BY
     [IssuePKey]
    ,[IssueAssigneeStartDateTime];
