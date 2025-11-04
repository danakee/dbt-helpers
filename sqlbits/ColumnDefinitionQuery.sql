SELECT 
    s.name AS [Schema],
    t.name AS [TableName],
    c.name AS [ColumnName],
    CASE
        -- User-defined types (simple handling)
        WHEN ty.is_user_defined = 1 THEN
            QUOTENAME(OBJECT_SCHEMA_NAME(ty.user_type_id)) + N'.' + QUOTENAME(ty.name)

        -- Character / binary types with length
        WHEN ty.name IN (N'char', N'varchar', N'binary', N'varbinary') THEN
            ty.name + N'(' +
                CASE 
                    WHEN c.max_length = -1 THEN N'MAX'
                    ELSE CAST(c.max_length AS NVARCHAR(10))
                END + N')'

        -- Unicode types (length is in bytes, so divide by 2)
        WHEN ty.name IN (N'nchar', N'nvarchar') THEN
            ty.name + N'(' +
                CASE 
                    WHEN c.max_length = -1 THEN N'MAX'
                    ELSE CAST(c.max_length / 2 AS NVARCHAR(10))
                END + N')'

        -- Decimal / numeric
        WHEN ty.name IN (N'decimal', N'numeric') THEN
            ty.name + N'(' 
            + CAST(c.precision AS NVARCHAR(10)) 
            + N',' 
            + CAST(c.scale AS NVARCHAR(10)) 
            + N')'

        -- Date/time with scale
        WHEN ty.name IN (N'datetime2', N'datetimeoffset', N'time') THEN
            ty.name + N'(' + CAST(c.scale AS NVARCHAR(10)) + N')'

        -- Everything else (int, bigint, bit, datetime, float, etc.)
        ELSE
            ty.name
    END AS [DataType]
FROM 
    sys.tables AS t
    INNER JOIN sys.schemas AS s
        ON t.schema_id = s.schema_id
    INNER JOIN sys.columns AS c
        ON c.object_id = t.object_id
    INNER JOIN sys.types AS ty
        ON c.user_type_id = ty.user_type_id
WHERE
    t.is_ms_shipped = 0
ORDER BY
    s.name,
    t.name,
    c.column_id;
