-- =============================================================================
-- FILE:    DimDate_IntegrityTests.sql
-- PURPOSE: Integrity validation suite for [dbo].[DimDate] in SimulationsAnalytics
-- AUTHOR:  SimulationsAnalytics EDW
-- DATE:    2026-03-23
--
-- RUN ORDER:
--   1. Duplicate Keys          -- if this fails, all other results are suspect
--   2. Contiguity              -- must return zero rows (no gaps)
--   3. DateSKey Format         -- confirms YYYYMMDD key derivation
--   4. Calendar Attributes     -- confirms ETL-derived calendar columns
--   5. Accounting Integrity    -- 12 periods, one start/end flag each, no overlaps
--   6. Date Range Coverage     -- confirms fact data is fully contained in DimDate
--   7. NULL / Sentinel Rows    -- informational; confirms sentinel row intent
--
-- EXPECTED RESULT: All queries return zero rows except #6 (range summary).
-- =============================================================================

USE [SimulationsAnalytics];
GO


-- =============================================================================
-- TEST 1: DUPLICATE KEYS
-- Both DateSKey and CalendarDate must be unique.
-- Any rows returned here indicate a broken primary key or ETL insert defect.
-- =============================================================================

-- 1a. Duplicate DateSKey values
SELECT
    [DateSKey]
   ,COUNT(*)                            AS [RowCount]
FROM 
    [SimulationsAnalytics].[dbo].[DimDate]
GROUP BY 
    [DateSKey]
HAVING 
    COUNT(*) > 1
ORDER BY 
    [DateSKey];

-- 1b. Duplicate CalendarDate values
SELECT
    [CalendarDate]
   ,COUNT(*)                            AS [RowCount]
FROM 
    [SimulationsAnalytics].[dbo].[DimDate]
GROUP BY 
    [CalendarDate]
HAVING 
    COUNT(*) > 1
ORDER BY 
    [CalendarDate];


-- =============================================================================
-- TEST 2: CONTIGUITY — NO MISSING DAYS
-- The most critical test for DAX time intelligence.
-- DimDate must have one row per calendar day with no gaps.
-- Any row returned indicates a missing date that will break TI navigation.
-- =============================================================================

SELECT
    [CalendarDate]                              AS [GapAfterThisDate]
   ,DATEADD( DAY, 1, [CalendarDate] )          AS [ExpectedNextDate]
   ,LEAD( [CalendarDate], 1 ) OVER (
        ORDER BY [CalendarDate]
    )                                           AS [ActualNextDate]
FROM 
    [SimulationsAnalytics].[dbo].[DimDate]
HAVING
    LEAD( [CalendarDate], 1 ) OVER (
        ORDER BY [CalendarDate]
    ) <> DATEADD( DAY, 1, [CalendarDate] )
ORDER BY 
    [CalendarDate];


-- =============================================================================
-- TEST 3: DATEKEY FORMAT INTEGRITY
-- Confirms that DateSKey is always the YYYYMMDD integer representation
-- of CalendarDate. Any mismatch indicates an ETL derivation error.
-- =============================================================================

SELECT
    [DateSKey]
   ,[CalendarDate]
   ,CONVERT( INT, FORMAT( [CalendarDate], 'yyyyMMdd' ) )  AS [ExpectedSKey]
FROM 
    [SimulationsAnalytics].[dbo].[DimDate]
WHERE 
    [DateSKey] <> CONVERT( INT, FORMAT( [CalendarDate], 'yyyyMMdd' ) )
ORDER BY 
    [CalendarDate];


-- =============================================================================
-- TEST 4: CALENDAR ATTRIBUTE CONSISTENCY
-- Derived columns (Year, Month, Day, DayOfWeek, WeekOfYear) must match
-- what SQL Server computes directly from CalendarDate.
-- Any rows returned indicate ETL calculation errors.
-- =============================================================================

SELECT
    [CalendarDate]
   ,[Year]
   ,YEAR( [CalendarDate] )                     AS [ExpectedYear]
   ,[Month]
   ,MONTH( [CalendarDate] )                    AS [ExpectedMonth]
   ,[Day]
   ,DAY( [CalendarDate] )                      AS [ExpectedDay]
   ,[DayOfWeek]
   ,DATEPART( WEEKDAY, [CalendarDate] )        AS [ExpectedDayOfWeek]
   ,[WeekOfYear]
   ,DATEPART( WEEK, [CalendarDate] )           AS [ExpectedWeekOfYear]
FROM 
    [SimulationsAnalytics].[dbo].[DimDate]
WHERE
       [Year]       <> YEAR( [CalendarDate] )
    OR [Month]      <> MONTH( [CalendarDate] )
    OR [Day]        <> DAY( [CalendarDate] )
    OR [DayOfWeek]  <> DATEPART( WEEKDAY, [CalendarDate] )
    OR [WeekOfYear] <> DATEPART( WEEK, [CalendarDate] )
ORDER BY 
    [CalendarDate];


-- =============================================================================
-- TEST 5: ACCOUNTING PERIOD INTEGRITY
-- FlightSafety uses a 12-period fiscal calendar that does not align with
-- calendar year boundaries. These tests validate the accounting calendar
-- structure that all TI measures will depend on.
-- =============================================================================

-- 5a. Each accounting period must have exactly one start date flag
--     and exactly one end date flag.
--     Any period with 0 or 2+ flags indicates an ETL defect.
SELECT
     [AccountingYear]
    ,[AccountingPeriod]
    ,COUNT(*)                                               AS [TotalDays]
    ,SUM( CAST( [IsAccountingPeriodStartDate] AS INT ) )   AS [StartDateFlags]
    ,SUM( CAST( [IsAccountingPeriodEndDate]   AS INT ) )   AS [EndDateFlags]
FROM 
    [SimulationsAnalytics].[dbo].[DimDate]
GROUP BY 
     [AccountingYear]
    ,[AccountingPeriod]
HAVING
       SUM( CAST( [IsAccountingPeriodStartDate] AS INT ) ) <> 1
    OR SUM( CAST( [IsAccountingPeriodEndDate]   AS INT ) ) <> 1
ORDER BY 
     [AccountingYear]
    ,[AccountingPeriod];

-- 5b. Each accounting year must have exactly 12 periods.
--     Any year with a different count indicates a missing or duplicate period.
SELECT
     [AccountingYear]
    ,COUNT( DISTINCT [AccountingPeriod] )      AS [PeriodCount]
FROM 
    [SimulationsAnalytics].[dbo].[DimDate]
GROUP BY 
    [AccountingYear]
HAVING 
    COUNT( DISTINCT [AccountingPeriod] ) <> 12
ORDER BY 
    [AccountingYear];

-- 5c. Accounting period end dates must not overlap with the next period's
--     start date. Any rows returned indicate a boundary overlap defect.
SELECT
     a.[AccountingYear]
    ,a.[AccountingPeriod]
    ,a.[AccountingPeriodEndDate]
    ,b.[AccountingPeriod]                      AS [NextPeriod]
    ,b.[AccountingPeriodStartDate]
FROM 
    [SimulationsAnalytics].[dbo].[DimDate] AS [a]
    JOIN [SimulationsAnalytics].[dbo].[DimDate] AS [b]
        ON  b.[AccountingYear]   = a.[AccountingYear]
        AND b.[AccountingPeriod] = a.[AccountingPeriod] + 1
WHERE 
    a.[AccountingPeriodEndDate] >= b.[AccountingPeriodStartDate]
GROUP BY
     a.[AccountingYear]
    ,a.[AccountingPeriod]
    ,a.[AccountingPeriodEndDate]
    ,b.[AccountingPeriod]
    ,b.[AccountingPeriodStartDate]
ORDER BY 
     a.[AccountingYear]
    ,a.[AccountingPeriod];


-- =============================================================================
-- TEST 6: DATE RANGE COVERAGE
-- DimDate must cover the full range of dates present in all fact tables.
-- This query returns a summary row per source — not a pass/fail test.
-- Verify that DimDate MinDate <= all fact MinDates
--          and DimDate MaxDate >= all fact MaxDates.
-- Add additional UNION ALL blocks for each fact table as needed.
-- =============================================================================

SELECT
    'DimDate'                                          AS [Source]
   ,MIN( [CalendarDate] )                             AS [MinDate]
   ,MAX( [CalendarDate] )                             AS [MaxDate]
   ,COUNT(*)                                          AS [RowCount]
FROM 
    [SimulationsAnalytics].[dbo].[DimDate]

UNION ALL

SELECT
    'FactProjectCostEstimate - AccountingPeriodSKey'
    ,MIN( d.[CalendarDate] )
    ,MAX( d.[CalendarDate] )
    ,COUNT( DISTINCT f.[AccountingPeriodSKey] )
FROM 
    [SimulationsAnalytics].[dbo].[FactProjectCostEstimate] [f]
    JOIN [dbo].[DimDate] AS [d]
        ON d.[DateSKey] = f.[AccountingPeriodSKey]

UNION ALL

SELECT
    'FactProjectCostEstimate - CreateDateSKey'
    ,MIN( d.[CalendarDate] )
    ,MAX( d.[CalendarDate] )
    ,COUNT( DISTINCT f.[CreateDateSKey] )
FROM 
    [SimulationsAnalytics].[dbo].[FactProjectCostEstimate] AS f
    JOIN [SimulationsAnalytics].[dbo].[DimDate] AS d
        ON d.[DateSKey] = f.[CreateDateSKey]
ORDER BY 
    [Source];


-- =============================================================================
-- TEST 7: NULL / SENTINEL ROW CHECK
-- Checks for rows with NULL key columns or non-positive DateSKey values.
-- A DateSKey of 0 or -1 is a valid EDW sentinel for "unknown date" —
-- but it must be intentional. Any unexpected rows here need investigation.
-- =============================================================================

SELECT
     [DateSKey]
    ,[CalendarDate]
    ,[AccountingYear]
    ,[AccountingPeriod]
FROM 
    [SimulationsAnalytics].[dbo].[DimDate]
WHERE
       [DateSKey]        <= 0
    OR [CalendarDate]    IS NULL
    OR [AccountingYear]  IS NULL
    OR [AccountingPeriod] IS NULL
ORDER BY 
    [DateSKey];
