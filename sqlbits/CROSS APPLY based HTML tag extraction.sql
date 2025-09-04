WITH [TagExtractor] AS (
SELECT 
    id,  -- Your primary key column
    note,
    SUBSTRING(note, 
        CHARINDEX('<', note, pos.number) + 1,
        CHARINDEX('>', note, CHARINDEX('<', note, pos.number)) - CHARINDEX('<', note, pos.number) - 1
    ) AS tag_content,
    '<' + SUBSTRING(note, 
        CHARINDEX('<', note, pos.number) + 1,
        CHARINDEX('>', note, CHARINDEX('<', note, pos.number)) - CHARINDEX('<', note, pos.number) - 1
    ) + '>' AS full_tag
FROM 
    [your_table]
CROSS APPLY (
    SELECT 
        [number] 
    FROM 
        [master]..[spt_values] 
    WHERE 
        [type] = 'P' 
        AND [number] BETWEEN 1 AND LEN(note)
        AND SUBSTRING(note, [number], 1) = '<') AS [pos]
WHERE CHARINDEX('>', note, CHARINDEX('<', note, [pos].[number])) > 0
)
SELECT 
    [full_tag],
    COUNT(*) as [tag_count]
FROM 
    [TagExtractor]
GROUP BY 
    [full_tag]
ORDER BY 
    [tag_count] DESC;