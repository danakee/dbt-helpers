CREATE OR ALTER PROCEDURE dbo.usp_GetDimFactTableMetadata
AS
BEGIN
    SET NOCOUNT ON;

    -- Temp table to hold results
    IF OBJECT_ID('tempdb..#TableMetadata') IS NOT NULL
        DROP TABLE #TableMetadata;

    CREATE TABLE #TableMetadata (
        SchemaName NVARCHAR(128),
        TableName NVARCHAR(128),
        RowCount BIGINT,
        MaxEDWCreateDatetime DATETIME2(7),
        MaxEDWLastUpdatedDateTime DATETIME2(7)
    );

    DECLARE @SQL NVARCHAR(MAX) = '';

    -- Build dynamic SQL for all Dim and Fact tables
    SELECT @SQL = @SQL + 
        'INSERT INTO #TableMetadata (SchemaName, TableName, RowCount, MaxEDWCreateDatetime, MaxEDWLastUpdatedDateTime)
        SELECT 
            ''' + s.name + ''' AS SchemaName,
            ''' + t.name + ''' AS TableName,
            COUNT(*) AS RowCount,
            MAX([EDWCreateDatetime]) AS MaxEDWCreateDatetime,
            MAX([EDWLastUpdatedDateTime]) AS MaxEDWLastUpdatedDateTime
        FROM [' + s.name + '].[' + t.name + '];
        '
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE (t.name LIKE 'Dim%' OR t.name LIKE 'Fact%')
        AND EXISTS (
            SELECT 1 
            FROM sys.columns c 
            WHERE c.object_id = t.object_id 
                AND c.name IN ('EDWCreateDatetime', 'EDWLastUpdatedDateTime')
        )
    ORDER BY t.name;

    -- Execute dynamic SQL
    EXEC sp_executesql @SQL;

    -- Return results
    SELECT 
        SchemaName,
        TableName,
        RowCount,
        MaxEDWCreateDatetime,
        MaxEDWLastUpdatedDateTime
    FROM #TableMetadata
    ORDER BY SchemaName, TableName;

END;
GO