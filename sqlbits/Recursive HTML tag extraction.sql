WITH [TagExtractor] AS (
    -- Anchor: Find first tag
    SELECT 
        id,
        note,
        CASE 
            WHEN CHARINDEX('<', note) > 0 AND CHARINDEX('>', note, CHARINDEX('<', note)) > 0
            THEN SUBSTRING(note, CHARINDEX('<', note), 
                          CHARINDEX('>', note, CHARINDEX('<', note)) - CHARINDEX('<', note) + 1)
            ELSE NULL
        END AS extracted_tag,
        CASE 
            WHEN CHARINDEX('<', note) > 0 AND CHARINDEX('>', note, CHARINDEX('<', note)) > 0
            THEN SUBSTRING(note, CHARINDEX('>', note, CHARINDEX('<', note)) + 1, LEN(note))
            ELSE ''
        END AS remaining_text,
        1 AS tag_position
    FROM 
        [your_table]
    WHERE 
        note LIKE '%<%>%'
    
    UNION ALL
    
    -- Recursive: Find next tag
    SELECT 
        id,
        note,
        CASE 
            WHEN CHARINDEX('<', remaining_text) > 0 AND CHARINDEX('>', remaining_text, CHARINDEX('<', remaining_text)) > 0
            THEN SUBSTRING(remaining_text, CHARINDEX('<', remaining_text), 
                          CHARINDEX('>', remaining_text, CHARINDEX('<', remaining_text)) - CHARINDEX('<', remaining_text) + 1)
            ELSE NULL
        END,
        CASE 
            WHEN CHARINDEX('<', remaining_text) > 0 AND CHARINDEX('>', remaining_text, CHARINDEX('<', remaining_text)) > 0
            THEN SUBSTRING(remaining_text, CHARINDEX('>', remaining_text, CHARINDEX('<', remaining_text)) + 1, LEN(remaining_text))
            ELSE ''
        END,
        tag_position + 1
    FROM 
        [TagExtractor]
    WHERE 
        remaining_text LIKE '%<%>%' 
        AND tag_position < 100  -- Prevent infinite recursion
)
SELECT 
    extracted_tag,
    COUNT(*) AS frequency,
    COUNT(DISTINCT id) AS records_with_tag
FROM 
    [TagExtractor]
WHERE 
    extracted_tag IS NOT NULL
GROUP BY 
    extracted_tag
ORDER BY 
    frequency DESC;