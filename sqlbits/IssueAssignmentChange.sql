WITH [Raw] AS (
SELECT
     [nd].[fk_issue]     AS [IssuePkey]
    ,[nd].[prim_key]     AS [NodeDataPkey]
    ,[nd].[fk_assignid]  AS [NodeAssignId]
    ,[nd].[create_dt]    AS [NodeCreatedDate]
    ,LAG([nd].[fk_assignid]) OVER (
        PARTITION BY 
            [nd].[fk_issue]
        ORDER BY 
            [nd].[create_dt], [nd].[prim_key]
    ) AS [PrevAssignId]
FROM 
    [Fsi_Issues2].[dbo].[tblnodedata] AS [nd]
WHERE 
    [nd].[create_dt] >= '2007-01-01'  -- sargable equivalent of YEAR(create_dt) > 2006
),

[BaseData] AS (
SELECT
     [r].[IssuePkey]
    ,[r].[NodeDataPkey]
    ,[r].[NodeAssignId]
    ,[r].[NodeCreatedDate]
    ,CASE
        WHEN [r].[PrevAssignId] IS NULL
            OR [r].[PrevAssignId] <> [r].[NodeAssignId]
        THEN 1 ELSE 0
    END AS [IsNewAssignment]
FROM 
    [Raw] AS [r]
),

[BaseWithGroup] AS (
SELECT
     [b].[IssuePkey]
    ,[b].[NodeDataPkey]
    ,[b].[NodeAssignId]
    ,[b].[NodeCreatedDate]
    ,SUM([b].[IsNewAssignment]) OVER (
        PARTITION BY 
            [b].[IssuePkey]
        ORDER BY 
            [b].[NodeCreatedDate], [b].[NodeDataPkey]
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 
    ) AS [AssignmentGroup]
FROM 
    [BaseData] AS [b]
),

[Grouped] AS(
SELECT
     [IssuePkey]
    ,MIN([NodeDataPkey])    AS [NodeDataPkey]        -- first node key in the period
    ,[NodeAssignId]
    ,MIN([NodeCreatedDate]) AS [AssignmentStartDate]
    ,MAX([NodeCreatedDate]) AS [AssignmentEndDate]
    ,COUNT(*)               AS [AssignmentNodeCount]
FROM 
    [BaseWithGroup]
GROUP BY
     [IssuePkey]
    ,[NodeAssignId]
    ,[AssignmentGroup]
),

[Numbered] AS
(
SELECT
      [g].[IssuePkey]
    , [g].[NodeDataPkey]
    , [g].[NodeAssignId]
    , [g].[AssignmentStartDate]
    , [g].[AssignmentEndDate]
    , [g].[AssignmentNodeCount]
    , ROW_NUMBER() OVER (
          PARTITION BY [g].[IssuePkey]
          ORDER BY [g].[AssignmentStartDate], [g].[NodeDataPkey]
      )                    AS [IssueAssignmentSeqNum]
    , COUNT(*) OVER (
          PARTITION BY [g].[IssuePkey]
      )                    AS [IssueNodeCount]
FROM 
    [Grouped] AS [g]
)
SELECT
      [n].[IssuePkey]
    , [n].[NodeDataPkey]
    , [n].[NodeAssignId]
    , [n].[AssignmentStartDate]
    , [n].[AssignmentEndDate]
    , [n].[AssignmentNodeCount]
    , [n].[IssueAssignmentSeqNum]
    , CASE WHEN [n].[IssueAssignmentSeqNum] = 1 THEN 0 ELSE 1 END AS [AssignmentChanged]
    , [n].[IssueNodeCount]
FROM 
    [Numbered] AS [n]
WHERE 
    [n].[IssueNodeCount] > 1
ORDER BY
      [n].[IssuePkey]
    , [n].[IssueAssignmentSeqNum];
