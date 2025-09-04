WITH [Numbers] AS (
SELECT TOP 8000 
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as n
FROM 
    [sys].[objects] AS o1 
    CROSS JOIN [sys].[objects] AS o2
),
[TagPositions] AS (
SELECT 
    id,
    note,
    n AS start_pos,
    CHARINDEX('>', note, n) AS end_pos
FROM 
    [your_table]
    CROSS JOIN [Numbers]
WHERE 
    SUBSTRING(note, n, 1) = '<'
    AND CHARINDEX('>', note, n) > n
    AND n <= LEN(note)
)
SELECT 
    SUBSTRING(note, start_pos, end_pos - start_pos + 1) AS html_tag,
    COUNT(*) AS tag_count,
    COUNT(DISTINCT id) AS record_count
FROM 
    [TagPositions]
GROUP BY 
    SUBSTRING(note, start_pos, end_pos - start_pos + 1)
ORDER BY 
    tag_count DESC;