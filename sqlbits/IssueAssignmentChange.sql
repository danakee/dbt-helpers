WITH [BaseData] AS (
SELECT 
    [nd].[fk_issue] AS [IssuePkey],
    [nd].[prim_key] AS [NodeDataPkey],
    [nd].[fk_assignid] AS [NodeAssignId],
    [nd].[create_dt] AS [NodeCreatedDate],
    -- Create groups for consecutive assignments to same user
    SUM(CASE 
        WHEN LAG([nd].[fk_assignid]) OVER (PARTITION BY [nd].[fk_issue] ORDER BY [nd].[create_dt]) IS NULL 
            OR LAG([nd].[fk_assignid]) OVER (PARTITION BY [nd].[fk_issue] ORDER BY [nd].[create_dt]) != [nd].[fk_assignid] 
            THEN 1 
        ELSE 0 
    END) OVER (PARTITION BY [nd].[fk_issue] ORDER BY [nd].[create_dt]) AS [AssignmentGroup]
FROM 
    [Fsi_Issues2].[dbo].[tblnodedata] AS [nd]
WHERE 
    YEAR([nd].[create_dt]) > 2006
),
[Test] AS (
SELECT 
    [IssuePkey],
    MIN([NodeDataPkey]) AS [NodeDataPkey], -- Keep first node data key for the assignment period
    [NodeAssignId],
    MIN([NodeCreatedDate]) AS [AssignmentStartDate],
    MAX([NodeCreatedDate]) AS [AssignmentEndDate],
    COUNT(*) AS [AssignmentNodeCount],
    ROW_NUMBER() OVER (PARTITION BY [IssuePkey] ORDER BY MIN([NodeCreatedDate])) AS [IssueAssignmentSeqNum],
    -- Assignment change flag (1 for changes, 0 for first assignment)
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY [IssuePkey] ORDER BY MIN([NodeCreatedDate])) = 1 
            THEN 0  -- First assignment
        ELSE 1      -- Assignment changed
    END AS [AssignmentChanged]
FROM 
    [BaseData]
GROUP BY 
    [IssuePkey], 
    [NodeAssignId], 
    [AssignmentGroup]
)
SELECT 
    [IssuePkey],
    [NodeDataPkey],
    [NodeAssignId],
    [AssignmentStartDate],
    [AssignmentEndDate],
    [AssignmentNodeCount],
    [IssueAssignmentSeqNum],
    [AssignmentChanged],
    -- Add total issue assignment count
    COUNT(*) OVER (PARTITION BY [IssuePkey]) AS [IssueNodeCount]
FROM [Test]
WHERE 
    (SELECT COUNT(*) FROM [Test] t2 WHERE t2.[IssuePkey] = [Test].[IssuePkey]) > 1
ORDER BY 
    [IssuePkey],
    [IssueAssignmentSeqNum];
    