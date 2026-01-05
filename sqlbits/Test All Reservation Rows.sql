WITH [Audit] AS (
SELECT
    r.*,

    [ExpectedROTElapsedDays] =
        CASE
            -- Eligibility gating: if any of these are true, ROTElapsedDays should be NULL
            WHEN [r].[ReservationDateEnded] > [r].[ROTAsOfDate] THEN NULL
            WHEN ([r].[TrainingStatusId] = 7 AND [r].[RICDAT] IS NULL) THEN NULL   -- only if TrainingStatusId exists on fact
            WHEN [r].[ReservationDateEnded] IS NULL THEN NULL
            WHEN [r].[HourlyFlag] = 'Y' THEN NULL
            WHEN [r].[IsParentReservation_] = 'N' THEN NULL
            ELSE
                CASE
                    WHEN [r].[AHCCDT] IS NULL THEN NULL
                    -- CLOSED reservation: use source-system close date (RICDAT)
                    WHEN [r].[RICDAT] IS NOT NULL AND [r].[AHCCDT_Date] IS NOT NULL
                        THEN IIF([r].[RICDAT] >= [r].[AHCCDT_Date],
                            DATEDIFF(DAY, [r].[AHCCDT_Date], [r].[RICDAT])
                            - (DATEDIFF(WEEK, [r].[AHCCDT_Date], [r].[RICDAT])) * 2
                            + [r].[RICDATDateOffSet], 0)
                    -- OPEN reservation: use run-time "as-of" date (ROTAsOfDate)
                    WHEN [r].[RICDAT] IS NULL AND [r].[AHCCDT_Date] IS NOT NULL
                        THEN IIF([r].[ROTAsOfDate] >= [r].[AHCCDT_Date],
                            DATEDIFF(DAY, [r].[AHCCDT_Date], [r].[ROTAsOfDate])
                            - (DATEDIFF(WEEK, [r].[AHCCDT_Date], [r].[ROTAsOfDate])) * 2
                            + [r].[CurrentDateOffSet] + [r].[startDateOffSet], 0)
                    ELSE NULL
                END
        END,

    [IsMismatch] =
        CASE
            WHEN [r].[ROTElapsedDays] IS NULL AND
                 (CASE
                    WHEN [r].[ReservationDateEnded] > [r].[ROTAsOfDate] THEN NULL
                    WHEN ([r].[TrainingStatusId] = 7 AND [r].[RICDAT] IS NULL) THEN NULL
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
                  END) IS NULL
                THEN 0
            WHEN [r].[ROTElapsedDays] =
                 (CASE
                    WHEN [r].[ReservationDateEnded] > [r].[ROTAsOfDate] THEN NULL
                    WHEN ([r].[TrainingStatusId] = 7 AND [r].[RICDAT] IS NULL) THEN NULL
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
                  END)
                THEN 0
            ELSE 1
        END
FROM 
    [OperationsAnalytics].[dbo].[factReservation_Test] AS [r]
)
SELECT
    -- Keys / identifiers (add more if you need them)
    [Audit].[AHID],

    -- Critical dates / inputs
    [Audit].[AHCCDT],
    [Audit].[AHCCDT_Date],
    [Audit].[RICDAT],
    [Audit].[ReservationDateEnded],
    [Audit].[ROTAsOfDate],

    -- Offsets (since they influence outcomes)
    [Audit].[RICDATDateOffSet],
    [Audit].[CurrentDateOffSet],
    [Audit].[startDateOffSet],

    -- Actual vs expected
    [Audit].[ROTElapsedDays] AS ActualROTElapsedDays,
    [Audit].[ExpectedROTElapsedDays]
FROM 
    [Audit]
WHERE 
    [Audit].[IsMismatch] = 1
ORDER BY 
    [Audit].[AHID];