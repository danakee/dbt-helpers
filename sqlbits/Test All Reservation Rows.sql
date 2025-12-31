DECLARE @ROTAsOfDate date =
CASE
    WHEN DATEPART(HOUR, GETDATE()) BETWEEN 12 AND 23 THEN CAST(GETDATE() as date)
    ELSE DATEADD(day, -1, CAST(GETDATE() as date))
END;

;WITH Expected AS (
SELECT
    r.*,

    ExpectedROTElapsedDays =
    CASE
        WHEN r.ReservationDateEnded > @ROTAsOfDate THEN NULL
        WHEN (r.TrainingStatusId = 7 AND r.RICDAT IS NULL) THEN NULL  -- if you store status id on fact
        WHEN r.ReservationDateEnded IS NULL THEN NULL
        WHEN r.HourlyFlag = 'Y' THEN NULL
        WHEN r.IsParentReservation_ = 'N' THEN NULL
        ELSE
            CASE
                WHEN r.AHCCDT IS NULL THEN NULL

                WHEN r.RICDAT IS NOT NULL
                    THEN IIF(r.RICDAT >= r.AHCCDT_Date,
                        DATEDIFF(DAY, r.AHCCDT_Date, r.RICDAT)
                        - (DATEDIFF(WEEK, r.AHCCDT_Date, r.RICDAT)) * 2
                        + r.RICDATDateOffSet, 0)

                WHEN r.RICDAT IS NULL
                    THEN IIF(@ROTAsOfDate >= r.AHCCDT_Date,
                        DATEDIFF(DAY, r.AHCCDT_Date, @ROTAsOfDate)
                        - (DATEDIFF(WEEK, r.AHCCDT_Date, @ROTAsOfDate)) * 2
                        + r.CurrentDateOffSet + r.startDateOffSet, 0)

                ELSE NULL
            END
    END
FROM 
    [OperationsAnalytics].[dbo].[factReservation] AS [r]
)
SELECT
    -- key columns to identify rows
    Expected.AHID,
    Expected.AHCCDT,
    Expected.AHCCDT_Date,
    Expected.RICDAT,
    Expected.ReservationDateEnded,

    -- actual vs expected
    Expected.ROTElapsedDays AS ActualROTElapsedDays,
    Expected.ExpectedROTElapsedDays,

    -- mismatch flag
    CASE
        WHEN (Expected.ROTElapsedDays IS NULL AND Expected.ExpectedROTElapsedDays IS NULL) THEN 0
        WHEN (Expected.ROTElapsedDays = Expected.ExpectedROTElapsedDays) THEN 0
        ELSE 1
    END AS IsMismatch
FROM 
    Expected
WHERE
    CASE
        WHEN (Expected.ROTElapsedDays IS NULL AND Expected.ExpectedROTElapsedDays IS NULL) THEN 0
        WHEN (Expected.ROTElapsedDays = Expected.ExpectedROTElapsedDays) THEN 0
        ELSE 1
    END = 1
ORDER BY 
    Expected.AHID;
