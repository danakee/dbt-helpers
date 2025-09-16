WITH Ordered AS (
SELECT
    [nd].[fk_issue]      AS IssuePKey,
    [nd].[prim_key]      AS NodeDataPKey,
    [nd].[fk_assignid]   AS IssueAssigneePKey,
    [nd].[create_dt]     AS NodeCreateDateTime,
    -- order key to break ties on same timestamp
    ROW_NUMBER() OVER (
        PARTITION BY 
            [nd].[fk_issue]
        ORDER BY
             [nd].[create_dt]
            ,[nd].[prim_key]
    ) AS [rn],
    -- flag when assignee changes (or first row)
    CASE
        WHEN LAG([nd].[fk_assignid]) OVER (
            PARTITION BY 
                [nd].[fk_issue]
            ORDER BY 
                 [nd].[create_dt]
                ,[nd].[prim_key]
             ) IS NULL
            OR LAG([nd].[fk_assignid]) OVER (
                PARTITION BY 
                    [nd].[fk_issue]
                ORDER BY 
                     [nd].[create_dt]
                    ,[nd].[prim_key]
             ) <> [nd].[fk_assignid]
        THEN 1 ELSE 0
    END AS [ChangeFlag]
FROM 
    [Fsi_Issues2].[dbo].[tblnodedata] AS [nd]
WHERE 
    [nd].[create_dt] > '2007-01-01'
),

[Segs] AS (
-- build “segments”: consecutive rows with the same assignee
SELECT
     [IssuePKey]
    ,[NodeDataPKey]
    ,[IssueAssigneePKey]
    ,[NodeCreateDateTime]
    ,[rn]
    ,SUM([ChangeFlag]) OVER (
        PARTITION BY 
            [IssuePKey]
        ORDER BY 
            [rn]
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS [SegmentId]
FROM 
    [Ordered]
),

[Collapsed] AS (
-- one row per segment (assignee span) with its start time
SELECT
     [IssuePKey]
    ,[IssueAssigneePKey]
    ,MIN([NodeCreateDateTime]) AS [IssueAssigneeStartDateTime]
FROM 
    [Segs]
GROUP BY 
     [IssuePKey]
    ,[SegmentId]
    ,[IssueAssigneePKey]
),

[WithEnds] AS (
-- get the next segment's start as this segment's end
SELECT
    [c].[IssuePKey],
    [c].[IssueAssigneePKey],
    [c].[.IssueAssigneeStartDateTime],
    LEAD([c].[IssueAssigneeStartDateTime]) OVER (
        PARTITION BY 
            [c].[IssuePKey]
        ORDER BY 
            [c].[IssueAssigneeStartDateTime]
    ) AS [IssueAssigneeEndDateTime]
FROM 
    [Collapsed] AS [c]
)
SELECT
     [IssuePKey]
    ,[IssueAssigneePKey]
    ,[IssueAssigneeStartDateTime]
    ,[IssueAssigneeEndDateTime]
    ,DATEDIFF(MINUTE, [IssueAssigneeStartDateTime], [IssueAssigneeEndDateTime]) AS [IssueAssigneeDurationMinutes]
    ,DATEDIFF(HOUR,   [IssueAssigneeStartDateTime], [IssueAssigneeEndDateTime]) AS [IssueAssigneeDurationHours]
    ,DATEDIFF(DAY,    [IssueAssigneeStartDateTime], [IssueAssigneeEndDateTime]) AS [IssueAssigneeDurationDays]
FROM 
    [WithEnds]
ORDER BY 
     [IssuePKey]
    ,[IssueAssigneeStartDateTime];
