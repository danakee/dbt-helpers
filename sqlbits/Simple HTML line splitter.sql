SELECT 
    pkey,
    LTRIM(RTRIM(value)) as note
FROM your_table
CROSS APPLY STRING_SPLIT(REPLACE(original_column, '<p>', '|'), '|')
WHERE LEN(LTRIM(RTRIM(value))) > 10  -- Filter out very short/empty segments
ORDER BY pkey;