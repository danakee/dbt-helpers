-----------------------------------------------------------------------
-- Generate SELECT + INNER JOINs from a fact table to all dim tables
-- Uses FK metadata in sys.foreign_keys / sys.foreign_key_columns
-----------------------------------------------------------------------
DECLARE @FactSchema   sysname = N'dbo';
DECLARE @FactTable    sysname = N'FactIssueActivity';   -- <= change this
DECLARE @Sql          nvarchar(MAX);

;WITH FKInfo AS
(
    SELECT
          ROW_NUMBER() OVER (
              ORDER BY ref_s.name, ref_t.name, ref_c.name
          )                                   AS JoinNum
        , fk.name                             AS ForeignKeyName
        , fk_s.name                           AS FactSchemaName
        , fk_t.name                           AS FactTableName
        , fk_c.name                           AS FactColumnName     -- in fact
        , ref_s.name                          AS DimSchemaName
        , ref_t.name                          AS DimTableName
        , ref_c.name                          AS DimColumnName      -- in dim
    FROM sys.foreign_keys AS fk
    INNER JOIN sys.tables AS fk_t
        ON fk.parent_object_id = fk_t.object_id
    INNER JOIN sys.schemas AS fk_s
        ON fk_t.schema_id = fk_s.schema_id
    INNER JOIN sys.tables AS ref_t
        ON fk.referenced_object_id = ref_t.object_id
    INNER JOIN sys.schemas AS ref_s
        ON ref_t.schema_id = ref_s.schema_id
    INNER JOIN sys.foreign_key_columns AS fkc
        ON fk.object_id = fkc.constraint_object_id
    INNER JOIN sys.columns AS fk_c
        ON fk_c.object_id = fk_t.object_id
       AND fk_c.column_id = fkc.parent_column_id
    INNER JOIN sys.columns AS ref_c
        ON ref_c.object_id = ref_t.object_id
       AND ref_c.column_id = fkc.referenced_column_id
    WHERE
            fk_s.name = @FactSchema
        AND fk_t.name = @FactTable
        AND fk_t.name LIKE 'Fact%'          -- rule 1
        AND fk_c.name LIKE '%SKey'          -- rule 4 (fact key)
        AND ref_c.name LIKE '%SKey'         -- rule 3 (dim key)
)
, JoinLines AS
(
    SELECT STRING_AGG(
               'INNER JOIN [' + DimSchemaName + '].[' + DimTableName + '] AS d'
               + CAST(JoinNum AS varchar(10)) + CHAR(13) + CHAR(10)
               + '    ON d' + CAST(JoinNum AS varchar(10)) + '.[' + DimColumnName + ']'
               + ' = f.[' + FactColumnName + ']'
           , CHAR(13) + CHAR(10)
           ) WITHIN GROUP (ORDER BY JoinNum) AS JoinText
    FROM FKInfo
)
SELECT @Sql =
    'SELECT' + CHAR(13) + CHAR(10) +
    '    f.*' + CHAR(13) + CHAR(10) +
    'FROM [' + @FactSchema + '].[' + @FactTable + '] AS f' + CHAR(13) + CHAR(10) +
    COALESCE((SELECT JoinText FROM JoinLines), '-- No dimension joins found')
;

-- See the generated statement
SELECT @Sql AS GeneratedSelect;
-- Or: PRINT @Sql (may get truncated if very long)
