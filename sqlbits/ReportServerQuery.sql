USE [ReportServer];
GO

CREATE OR ALTER VIEW dbo.vw_ReportExecutionTroubleshooting
AS
WITH SubscriptionAgg AS
(
    SELECT
        s.Report_OID                                          AS ReportItemID,
        COUNT_BIG(*)                                          AS SubscriptionCount,
        MAX(CASE WHEN s.LastStatus IS NOT NULL THEN 1 ELSE 0 END) AS HasSubscriptionStatus,
        MAX(s.LastRunTime)                                    AS LastSubscriptionRunTime,
        MAX(s.ModifiedDate)                                   AS LastSubscriptionModifiedDate,
        MAX(CASE WHEN s.EventType = 'TimedSubscription' THEN 1 ELSE 0 END) AS HasTimedSubscription,
        MAX(CASE WHEN s.EventType = 'SnapshotUpdated'  THEN 1 ELSE 0 END) AS HasSnapshotSchedule
    FROM dbo.Subscriptions AS s
    GROUP BY
        s.Report_OID
)
SELECT
    -- Execution grain
    el.InstanceName,
    el.ReportID,
    el.ExecutionId,
    el.TimeStart,
    el.TimeEnd,
    DATEDIFF(MILLISECOND, el.TimeStart, el.TimeEnd)           AS TotalElapsedMs,

    -- Execution outcome
    el.Status,
    CASE WHEN el.Status <> 'rsSuccess' THEN 1 ELSE 0 END      AS IsFailure,
    el.ByteCount,
    el.[RowCount],
    el.[Format],
    el.Source,
    el.RequestType,
    el.ItemAction,

    -- Performance breakdown
    el.TimeDataRetrieval,
    el.TimeProcessing,
    el.TimeRendering,

    -- Who / where
    el.UserName,
    el.Parameters,
    el.AdditionalInfo,

    -- Catalog metadata
    c.ItemID,
    c.[Path]                                                  AS CatalogPath,
    c.[Name]                                                  AS CatalogName,
    c.[Description],
    c.CreationDate,
    c.ModifiedDate,
    c.Hidden,
    c.[Type]                                                  AS CatalogTypeCode,

    -- Report family / item family
    CASE
        WHEN c.[Type] = 2  THEN 'Paginated (.rdl)'
        WHEN c.[Type] = 13 THEN 'Power BI (.pbix)'
        WHEN c.[Type] = 14 THEN 'Excel Workbook'
        WHEN c.[Type] = 3  THEN 'Resource'
        WHEN c.[Type] = 8  THEN 'Shared Dataset'
        WHEN c.[Type] = 5  THEN 'Shared Data Source'
        WHEN c.[Type] = 1  THEN 'Folder'
        ELSE CONCAT('Other / Unknown (Type=', c.[Type], ')')
    END                                                       AS ItemTypeLabel,

    CASE
        WHEN c.[Type] = 2  THEN 'RDL'
        WHEN c.[Type] = 13 THEN 'PBIX'
        WHEN c.[Type] = 14 THEN 'XLSX'
        ELSE 'OTHER'
    END                                                       AS ReportType,

    -- Scheduling / subscription context
    CAST(CASE WHEN ISNULL(sa.SubscriptionCount, 0) > 0 THEN 1 ELSE 0 END AS bit) AS HasAnySubscription,
    ISNULL(sa.SubscriptionCount, 0)                           AS SubscriptionCount,
    CAST(ISNULL(sa.HasTimedSubscription, 0) AS bit)          AS HasTimedSubscription,
    CAST(ISNULL(sa.HasSnapshotSchedule, 0) AS bit)           AS HasSnapshotSchedule,
    sa.LastSubscriptionRunTime,
    sa.LastSubscriptionModifiedDate,

    -- Was this specific execution schedule-driven?
    CAST
    (
        CASE
            WHEN el.RequestType IN ('Subscription', 'System') THEN 1
            ELSE 0
        END
        AS bit
    )                                                         AS IsScheduledExecution,

    CASE
        WHEN el.RequestType = 'Subscription' THEN 'Subscription'
        WHEN el.RequestType = 'System'       THEN 'System / background'
        WHEN el.RequestType = 'Interactive'  THEN 'Interactive'
        ELSE el.RequestType
    END                                                       AS ExecutionTriggerCategory

FROM 
    dbo.ExecutionLog3 AS el
    INNER JOIN dbo.Catalog AS c
        ON c.ItemID = el.ReportID
    LEFT JOIN SubscriptionAgg AS sa
        ON sa.ReportItemID = c.ItemID
WHERE
    c.[Type] IN (2, 13, 14);
GO