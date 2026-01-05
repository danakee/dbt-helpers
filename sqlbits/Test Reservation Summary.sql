WITH [Audit] AS (
SELECT
    [ExpectedROTElapsedDays] =
        CASE
            WHEN [r].[ReservationDateEnded] > [r].[ROTAsOfDate] THEN NULL
            WHEN [r].[ReservationDateEnded] IS NULL THEN NULL
            WHEN [r].[HourlyFlag] = 'Y' THEN NULL
            WHEN [r].[IsParentReservation_] = 'N' THEN NULL
            ELSE
                CASE
                    WHEN [r].[AHCCDT] IS NULL THEN NULL
                    WHEN [r].[RICDAT] IS NOT NULL AND [r].[AHCCDT_Date] IS NOT NULL
                        THEN IIF([r].[RICDAT] >= [r].[AHCCDT_Date],
                            DATEDIFF(DAY, [r].[AHCCDT_Date], [r].[RICDAT])
                            - (DATEDIFF(WEEK, [r].[AHCCDT_Date], [r].[RICDAT])) * 2
                            + [r].[RICDATDateOffSet], 0)
                    WHEN [r].[RICDAT] IS NULL AND [r].[AHCCDT_Date] IS NOT NULL
                        THEN IIF([r].[ROTAsOfDate] >= [r].[AHCCDT_Date],
                            DATEDIFF(DAY, [r].[AHCCDT_Date], [r].[ROTAsOfDate])
                            - (DATEDIFF(WEEK, [r].[AHCCDT_Date], [r].[ROTAsOfDate])) * 2
                            + [r].[CurrentDateOffSet] + [r].[startDateOffSet], 0)
                    ELSE NULL
                END
        END,
    [ActualROTElapsedDays] = [r].[ROTElapsedDays]
FROM 
    [OperationsAnalytics].[dbo].[factReservation_Test] AS [r]
)
SELECT
    [TotalRows] = COUNT(*),
    [MatchRows] =
        SUM(CASE
              WHEN [ActualROTElapsedDays] IS NULL AND [ExpectedROTElapsedDays] IS NULL THEN 1
              WHEN [ActualROTElapsedDays] = [ExpectedROTElapsedDays] THEN 1
              ELSE 0
            END),
    [MismatchRows] =
        SUM(CASE
              WHEN [ActualROTElapsedDays] IS NULL AND [ExpectedROTElapsedDays] IS NULL THEN 0
              WHEN [ActualROTElapsedDays] = [ExpectedROTElapsedDays] THEN 0
              ELSE 1
            END)
FROM 
    [Audit];
