DECLARE @ROTAsOfDate date =
CASE
    WHEN DATEPART(HOUR, GETDATE()) BETWEEN 12 AND 23 THEN CAST(GETDATE() as date)
    ELSE DATEADD(day, -1, CAST(GETDATE() as date))
END;

;WITH Expected AS (
SELECT
    ExpectedROTElapsedDays =
    -- (same CASE as above),
    r.ROTElapsedDays
FROM 
    [OperationsAnalytics].[dbo].[factReservation] AS [r]
)
SELECT
    TotalRows = COUNT(*),
    MismatchRows = SUM(CASE WHEN 1=1 /* replace with mismatch predicate */ THEN 1 ELSE 0 END);
