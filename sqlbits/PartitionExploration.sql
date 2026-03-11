/*
================================================================================
  SimulationsAnalytics — Partition Key Investigation Scripts
  Purpose : Pre-meeting data investigation to confirm partition key candidates,
            year distributions, and NULL rates for each qualifying fact table.
  Database: SimulationsAnalytics (run against DEV or UAT)
  Date    : March 11, 2026
================================================================================

  TABLES COVERED
  ──────────────
  A.2  FactTimesheet                  (12,579,348 rows)  Key: DateKey
  A.3  FactSimulatorIssueActivity     ( 8,981,713 rows)  Key: ActivityNoteCreatedDateKey
  A.4  FactProjectCostEstimate        ( 3,866,890 rows)  Key: AccountingPeriodDateKey (verify vs CreateDateKey)
  A.5  FactSimulatorIssueAssignment   ( 3,147,364 rows)  Key: IssueAssignmentStartDateKey (confirm name)
  A.6  FactSimulatorIssueStatus       ( 1,851,045 rows)  Key: StatusStartDateKey (confirm name)
  A.7  FactSimulatorIssueDetail       ( 1,278,160 rows)  Key: IssueCreatedDateKey

  DROPPED FROM CANDIDACY
  ──────────────────────
  FactSimulatorConfiguration          (  1,943 rows)  Too small
  FactSimulatorConfigurationComponent ( 17,976 rows)  Too small
  FactSimulatorConfigurationSubComp   ( 18,364 rows)  Too small
  FactSimulatorLocation               (  2,072 rows)  No DimDate FK
  FactSimulatorQualification          (  1,962 rows)  Too small despite 7 date FKs

  REUSABLE TEMPLATE (integer YYYYMMDD DateKey)
  ────────────────────────────────────────────
  -- Year distribution
  SELECT  [<DateKeyColumn>] / 10000  AS [Year],
          COUNT(*)                   AS RowCount
  FROM    [dbo].[<TableName>]
  WHERE   [<DateKeyColumn>] IS NOT NULL
  GROUP BY [<DateKeyColumn>] / 10000
  ORDER BY [Year];

  -- NULL rate
  SELECT  COUNT(*)                                                          AS TotalRows,
          SUM(CASE WHEN [<DateKeyColumn>] IS NULL THEN 1 ELSE 0 END)       AS NullKeyRows,
          CAST(SUM(CASE WHEN [<DateKeyColumn>] IS NULL THEN 1 ELSE 0 END)
               * 100.0 / COUNT(*) AS DECIMAL(5,2))                         AS NullKeyPct
  FROM    [dbo].[<TableName>];

================================================================================
*/

USE [SimulationsAnalytics];
GO

-- ============================================================================
-- A.2  FactTimesheet
--      Partition key : DateKey
--      Status        : Year distribution already confirmed (see planning doc).
--                      Run NULL check only.
-- ============================================================================

PRINT '== A.2 FactTimesheet — NULL check ==';

SELECT  COUNT(*)                                                              AS TotalRows,
        SUM(CASE WHEN [DateKey] IS NULL THEN 1 ELSE 0 END)                  AS NullKeyRows,
        CAST(SUM(CASE WHEN [DateKey] IS NULL THEN 1 ELSE 0 END)
             * 100.0 / COUNT(*) AS DECIMAL(5,2))                             AS NullKeyPct
FROM    [dbo].[FactTimesheet];
GO


-- ============================================================================
-- A.3  FactSimulatorIssueActivity
--      Partition key : ActivityNoteCreatedDateKey
--      Single DimDate FK — no ambiguity.
-- ============================================================================

PRINT '== A.3 FactSimulatorIssueActivity — Year distribution ==';

SELECT  [ActivityNoteCreatedDateKey] / 10000  AS [Year],
        COUNT(*)                              AS RowCount
FROM    [dbo].[FactSimulatorIssueActivity]
WHERE   [ActivityNoteCreatedDateKey] IS NOT NULL
GROUP BY [ActivityNoteCreatedDateKey] / 10000
ORDER BY [Year];
GO

PRINT '== A.3 FactSimulatorIssueActivity — NULL rate ==';

SELECT  COUNT(*)                                                                         AS TotalRows,
        SUM(CASE WHEN [ActivityNoteCreatedDateKey] IS NULL THEN 1 ELSE 0 END)           AS NullKeyRows,
        CAST(SUM(CASE WHEN [ActivityNoteCreatedDateKey] IS NULL THEN 1 ELSE 0 END)
             * 100.0 / COUNT(*) AS DECIMAL(5,2))                                        AS NullKeyPct
FROM    [dbo].[FactSimulatorIssueActivity];
GO


-- ============================================================================
-- A.4  FactProjectCostEstimate
--      Partition key candidates : AccountingPeriodDateKey  vs  CreateDateKey
--      Run BOTH distributions and compare before committing to a key.
--
--      Decision guide:
--        If distributions are similar       → use AccountingPeriodDateKey
--                                             (aligns with report filter patterns)
--        If CreateDateKey skews much later  → retroactive entry is common;
--                                             discuss with team whether frozen
--                                             AccountingPeriod partitions are safe
-- ============================================================================

PRINT '== A.4 FactProjectCostEstimate — AccountingPeriodDateKey distribution ==';

SELECT  [AccountingPeriodDateKey] / 10000  AS [Year],
        COUNT(*)                           AS RowCount
FROM    [dbo].[FactProjectCostEstimate]
WHERE   [AccountingPeriodDateKey] IS NOT NULL
GROUP BY [AccountingPeriodDateKey] / 10000
ORDER BY [Year];
GO

PRINT '== A.4 FactProjectCostEstimate — CreateDateKey distribution (compare with above) ==';

SELECT  [CreateDateKey] / 10000  AS [Year],
        COUNT(*)                 AS RowCount
FROM    [dbo].[FactProjectCostEstimate]
WHERE   [CreateDateKey] IS NOT NULL
GROUP BY [CreateDateKey] / 10000
ORDER BY [Year];
GO

PRINT '== A.4 FactProjectCostEstimate — NULL rates on both keys ==';

SELECT  COUNT(*)                                                                    AS TotalRows,
        SUM(CASE WHEN [AccountingPeriodDateKey] IS NULL THEN 1 ELSE 0 END)         AS NullAcctPeriod,
        CAST(SUM(CASE WHEN [AccountingPeriodDateKey] IS NULL THEN 1 ELSE 0 END)
             * 100.0 / COUNT(*) AS DECIMAL(5,2))                                   AS NullAcctPeriodPct,
        SUM(CASE WHEN [CreateDateKey]           IS NULL THEN 1 ELSE 0 END)         AS NullCreateDate,
        CAST(SUM(CASE WHEN [CreateDateKey] IS NULL THEN 1 ELSE 0 END)
             * 100.0 / COUNT(*) AS DECIMAL(5,2))                                   AS NullCreateDatePct
FROM    [dbo].[FactProjectCostEstimate];
GO


-- ============================================================================
-- A.5  FactSimulatorIssueAssignment
--      Partition key : IssueAssignmentStartDateKey  (confirm exact name below)
--
--      Note: EndDateKey IS NULL = open/active assignment.
--            Do NOT use EndDateKey as partition key — it is mutable and nullable.
-- ============================================================================

PRINT '== A.5 FactSimulatorIssueAssignment — confirm date column names ==';

SELECT  COLUMN_NAME,
        DATA_TYPE,
        IS_NULLABLE
FROM    INFORMATION_SCHEMA.COLUMNS
WHERE   TABLE_NAME   = 'FactSimulatorIssueAssignment'
  AND   TABLE_SCHEMA = 'dbo'
ORDER BY ORDINAL_POSITION;
GO

PRINT '== A.5 FactSimulatorIssueAssignment — StartDateKey year distribution ==';
-- !! Substitute confirmed column name if different from IssueAssignmentStartDateKey !!

SELECT  [IssueAssignmentStartDateKey] / 10000  AS [Year],
        COUNT(*)                               AS RowCount
FROM    [dbo].[FactSimulatorIssueAssignment]
WHERE   [IssueAssignmentStartDateKey] IS NOT NULL
GROUP BY [IssueAssignmentStartDateKey] / 10000
ORDER BY [Year];
GO

PRINT '== A.5 FactSimulatorIssueAssignment — NULL rate + open assignment sizing ==';

SELECT  COUNT(*)                                                                            AS TotalRows,
        SUM(CASE WHEN [IssueAssignmentStartDateKey] IS NULL THEN 1 ELSE 0 END)             AS NullStartKey,
        SUM(CASE WHEN [IssueAssignmentEndDateKey]   IS NULL THEN 1 ELSE 0 END)             AS OpenAssignments,
        CAST(SUM(CASE WHEN [IssueAssignmentEndDateKey] IS NULL THEN 1 ELSE 0 END)
             * 100.0 / COUNT(*) AS DECIMAL(5,2))                                            AS OpenAssignmentPct
FROM    [dbo].[FactSimulatorIssueAssignment];
GO


-- ============================================================================
-- A.6  FactSimulatorIssueStatus
--      Partition key : StatusStartDateKey  (confirm exact name below)
--
--      Three date FKs: IssueCreateDateKey, StatusStartDateKey, StatusEndDateKey
--      IssueCreateDateKey would misroute recent status changes to old partitions.
--      StatusEndDateKey is nullable (open/current status rows).
--      StatusStartDateKey is the event date for this record — recommended key.
-- ============================================================================

PRINT '== A.6 FactSimulatorIssueStatus — confirm date column names ==';

SELECT  COLUMN_NAME,
        DATA_TYPE,
        IS_NULLABLE
FROM    INFORMATION_SCHEMA.COLUMNS
WHERE   TABLE_NAME   = 'FactSimulatorIssueStatus'
  AND   TABLE_SCHEMA = 'dbo'
ORDER BY ORDINAL_POSITION;
GO

PRINT '== A.6 FactSimulatorIssueStatus — StatusStartDateKey year distribution ==';
-- !! Substitute confirmed column name if different from StatusStartDateKey !!

SELECT  [StatusStartDateKey] / 10000  AS [Year],
        COUNT(*)                      AS RowCount
FROM    [dbo].[FactSimulatorIssueStatus]
WHERE   [StatusStartDateKey] IS NOT NULL
GROUP BY [StatusStartDateKey] / 10000
ORDER BY [Year];
GO

PRINT '== A.6 FactSimulatorIssueStatus — NULL rates on all three date keys ==';

SELECT  COUNT(*)                                                                   AS TotalRows,
        SUM(CASE WHEN [IssueCreateDateKey]  IS NULL THEN 1 ELSE 0 END)            AS NullIssueCreate,
        CAST(SUM(CASE WHEN [IssueCreateDateKey]  IS NULL THEN 1 ELSE 0 END)
             * 100.0 / COUNT(*) AS DECIMAL(5,2))                                  AS NullIssueCreatePct,
        SUM(CASE WHEN [StatusStartDateKey]  IS NULL THEN 1 ELSE 0 END)            AS NullStatusStart,
        CAST(SUM(CASE WHEN [StatusStartDateKey]  IS NULL THEN 1 ELSE 0 END)
             * 100.0 / COUNT(*) AS DECIMAL(5,2))                                  AS NullStatusStartPct,
        SUM(CASE WHEN [StatusEndDateKey]    IS NULL THEN 1 ELSE 0 END)            AS NullStatusEnd,
        CAST(SUM(CASE WHEN [StatusEndDateKey]    IS NULL THEN 1 ELSE 0 END)
             * 100.0 / COUNT(*) AS DECIMAL(5,2))                                  AS NullStatusEndPct
FROM    [dbo].[FactSimulatorIssueStatus];
GO


-- ============================================================================
-- A.7  FactSimulatorIssueDetail
--      Partition key : IssueCreatedDateKey
--
--      Five DimDate FKs:
--        IssueCreatedDateKey  ← recommended (immutable, always populated)
--        IssueDateKey
--        IssueClosedDateKey   ← nullable (open issues)
--        IssueDueDateKey      ← nullable
--        IssueModifiedDateKey ← volatile (changes on every edit)
--
--      Also verify grain: DimSimulatorIssue = 1,278,155 rows
--                         FactSimulatorIssueDetail = 1,278,160 rows  (delta of 5)
--      Expected: 1 row per issue.  If DistinctIssues < TotalRows, grain is not 1:1.
-- ============================================================================

PRINT '== A.7 FactSimulatorIssueDetail — IssueCreatedDateKey year distribution ==';

SELECT  [IssueCreatedDateKey] / 10000  AS [Year],
        COUNT(*)                       AS RowCount
FROM    [dbo].[FactSimulatorIssueDetail]
WHERE   [IssueCreatedDateKey] IS NOT NULL
GROUP BY [IssueCreatedDateKey] / 10000
ORDER BY [Year];
GO

PRINT '== A.7 FactSimulatorIssueDetail — NULL rates on all five date keys ==';

SELECT  COUNT(*)                                                                   AS TotalRows,
        SUM(CASE WHEN [IssueCreatedDateKey]  IS NULL THEN 1 ELSE 0 END)           AS NullIssueCreated,
        CAST(SUM(CASE WHEN [IssueCreatedDateKey]  IS NULL THEN 1 ELSE 0 END)
             * 100.0 / COUNT(*) AS DECIMAL(5,2))                                  AS NullIssueCreatedPct,
        SUM(CASE WHEN [IssueDateKey]         IS NULL THEN 1 ELSE 0 END)           AS NullIssueDate,
        SUM(CASE WHEN [IssueClosedDateKey]   IS NULL THEN 1 ELSE 0 END)           AS NullIssueClosed,
        CAST(SUM(CASE WHEN [IssueClosedDateKey]   IS NULL THEN 1 ELSE 0 END)
             * 100.0 / COUNT(*) AS DECIMAL(5,2))                                  AS NullIssueClosedPct,
        SUM(CASE WHEN [IssueDueDateKey]      IS NULL THEN 1 ELSE 0 END)           AS NullIssueDue,
        SUM(CASE WHEN [IssueModifiedDateKey] IS NULL THEN 1 ELSE 0 END)           AS NullIssueModified
FROM    [dbo].[FactSimulatorIssueDetail];
GO

PRINT '== A.7 FactSimulatorIssueDetail — Grain check (expect TotalRows ≈ DistinctIssues) ==';

SELECT  COUNT(*)                              AS TotalRows,
        COUNT(DISTINCT [SimulatorIssueKey])   AS DistinctIssues,
        COUNT(*) - COUNT(DISTINCT [SimulatorIssueKey])  AS RowsAboveGrain
FROM    [dbo].[FactSimulatorIssueDetail];
GO


-- ============================================================================
-- BONUS — Cross-table summary: all candidates, row counts, earliest data year
--         Run this first for a quick orientation before diving into each table.
-- ============================================================================

PRINT '== SUMMARY — Earliest and latest year per candidate table ==';

SELECT 'FactTimesheet'               AS TableName, MIN([DateKey]) / 10000 AS EarliestYear, MAX([DateKey]) / 10000 AS LatestYear FROM [dbo].[FactTimesheet]              WHERE [DateKey] IS NOT NULL
UNION ALL
SELECT 'FactSimulatorIssueActivity'  AS TableName, MIN([ActivityNoteCreatedDateKey]) / 10000, MAX([ActivityNoteCreatedDateKey]) / 10000 FROM [dbo].[FactSimulatorIssueActivity] WHERE [ActivityNoteCreatedDateKey] IS NOT NULL
UNION ALL
SELECT 'FactProjectCostEstimate'     AS TableName, MIN([AccountingPeriodDateKey]) / 10000,    MAX([AccountingPeriodDateKey]) / 10000    FROM [dbo].[FactProjectCostEstimate]    WHERE [AccountingPeriodDateKey] IS NOT NULL
UNION ALL
SELECT 'FactSimulatorIssueDetail'    AS TableName, MIN([IssueCreatedDateKey]) / 10000,        MAX([IssueCreatedDateKey]) / 10000        FROM [dbo].[FactSimulatorIssueDetail]   WHERE [IssueCreatedDateKey] IS NOT NULL
-- Add FactSimulatorIssueAssignment and FactSimulatorIssueStatus after confirming column names
ORDER BY EarliestYear;
GO