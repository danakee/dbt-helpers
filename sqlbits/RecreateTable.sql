USE [SimulationsAnalyticsLogging];
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;

--------------------------------------------------------------------------------
-- Safety checks
--------------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.DBTProcessExecutionLog', N'U') IS NULL
BEGIN
    RAISERROR('Table [dbo].[DBTProcessExecutionLog] does not exist.', 16, 1);
    RETURN;
END;

-- Optional: don’t overwrite an unexpected backup
IF OBJECT_ID(N'dbo.DBTProcessExecutionLog_Backup', N'U') IS NOT NULL
BEGIN
    RAISERROR('Backup table [dbo].[DBTProcessExecutionLog_Backup] already exists. Aborting.', 16, 1);
    RETURN;
END;

--------------------------------------------------------------------------------
-- 1. Create backup OUTSIDE the transaction so it survives rollback
--------------------------------------------------------------------------------
SELECT *
INTO dbo.DBTProcessExecutionLog_Backup
FROM dbo.DBTProcessExecutionLog;

--------------------------------------------------------------------------------
-- 2. Drop & recreate inside a transaction
--------------------------------------------------------------------------------
BEGIN TRY
    BEGIN TRAN;

    -------------------------------------------------------------------------
    -- Drop original table (this will be rolled back if anything fails)
    -------------------------------------------------------------------------
    DROP TABLE dbo.DBTProcessExecutionLog;

    -------------------------------------------------------------------------
    -- Recreate table with new definition
    -------------------------------------------------------------------------
    CREATE TABLE [dbo].[DBTProcessExecutionLog] (
        [ProcessId]          [INT] IDENTITY(1,1) NOT NULL,
        [InvocationGUID]     [UNIQUEIDENTIFIER]  NOT NULL,
        [ProcessGUID]        [UNIQUEIDENTIFIER]  NOT NULL,
             NOT NULL,
            NOT NULL,
            NOT NULL,
        [IsFullRefresh]      [BIT]               NOT NULL,
              NOT NULL,
            NULL,
        [InitialRowCount]    [INT]               NOT NULL,
        [RowsDeleted]        [INT]               NULL,
        [RowsInserted]       [INT]               NULL,
        [RowsUpdated]        [INT]               NULL,
        [FinalRowCount]      [INT]               NULL,
         NOT NULL,
         NULL,
        -- >>> updated calculation goes here <<<
        [DurationMinutes]    AS (DATEDIFF(SECOND, [ProcessStartTime], [ProcessEndTime]) / (60.0)) PERSISTED,
        [DurationSeconds]    AS (DATEDIFF(SECOND, [ProcessStartTime], [ProcessEndTime])) PERSISTED,
        [DurationMilliseconds] AS (DATEDIFF(MILLISECOND, [ProcessStartTime], [ProcessEndTime])) PERSISTED,
        [ErrorNumber]        [INT]               NULL,
        [ErrorSeverity]      [INT]               NULL,
        [ErrorState]         [INT]               NULL,
        CONSTRAINT [PK_DBTProcessExecutionLog]
            PRIMARY KEY CLUSTERED
            (
                [ProcessId] ASC
            )
            WITH (
                PAD_INDEX = OFF,
                STATISTICS_NORECOMPUTE = OFF,
                IGNORE_DUP_KEY = OFF,
                ALLOW_ROW_LOCKS = ON,
                ALLOW_PAGE_LOCKS = ON,
                OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
            ) ON [PRIMARY]
    ) ON [PRIMARY];

    -------------------------------------------------------------------------
    -- 3. Reload data from backup, preserving identity values
    -------------------------------------------------------------------------
    SET IDENTITY_INSERT dbo.DBTProcessExecutionLog ON;

    INSERT INTO dbo.DBTProcessExecutionLog (
        [ProcessId],
        [InvocationGUID],
        [ProcessGUID],
        [ProcessName],
        [SourceTable],
        [TargetTable],
        [IsFullRefresh],
        [ExecutionStatus],
        [ExecutionMessage],
        [InitialRowCount],
        [RowsDeleted],
        [RowsInserted],
        [RowsUpdated],
        [FinalRowCount],
        [ProcessStartTime],
        [ProcessEndTime],
        [ErrorNumber],
        [ErrorSeverity],
        [ErrorState]
    )
    SELECT
        [ProcessId],
        [InvocationGUID],
        [ProcessGUID],
        [ProcessName],
        [SourceTable],
        [TargetTable],
        [IsFullRefresh],
        [ExecutionStatus],
        [ExecutionMessage],
        [InitialRowCount],
        [RowsDeleted],
        [RowsInserted],
        [RowsUpdated],
        [FinalRowCount],
        [ProcessStartTime],
        [ProcessEndTime],
        [ErrorNumber],
        [ErrorSeverity],
        [ErrorState]
    FROM dbo.DBTProcessExecutionLog_Backup
    ORDER BY [ProcessId];

    SET IDENTITY_INSERT dbo.DBTProcessExecutionLog OFF;

    -------------------------------------------------------------------------
    -- 4. Sanity check: row counts must match
    -------------------------------------------------------------------------
    DECLARE @SourceCount INT, @TargetCount INT;

    SELECT @SourceCount = COUNT(*) FROM dbo.DBTProcessExecutionLog_Backup;
    SELECT @TargetCount = COUNT(*) FROM dbo.DBTProcessExecutionLog;

    IF @SourceCount <> @TargetCount
    BEGIN
        RAISERROR(
            'Row count mismatch between backup and recreated table (%d vs %d).',
            16, 1, @SourceCount, @TargetCount
        );
    END;

    -------------------------------------------------------------------------
    -- 5. Commit the changes
    -------------------------------------------------------------------------
    COMMIT;

    -------------------------------------------------------------------------
    -- 6. Only after a successful commit, drop the backup
    -------------------------------------------------------------------------
    DROP TABLE dbo.DBTProcessExecutionLog_Backup;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK;

    -- Leave the backup table in place for inspection/recovery.
    DECLARE
        @ErrorMessage  NVARCHAR(4000) = ERROR_MESSAGE(),
        @ErrorSeverity INT = ERROR_SEVERITY(),
        @ErrorState    INT = ERROR_STATE();

    RAISERROR(
        'Error recreating [dbo].[DBTProcessExecutionLog]: %s',
        @ErrorSeverity,
        @ErrorState,
        @ErrorMessage
    );
END CATCH;
