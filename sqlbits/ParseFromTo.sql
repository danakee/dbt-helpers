SELECT
    t.FromToText,
    FromText = LTRIM(RTRIM(SUBSTRING(s, pos_from + 6, pos_to - pos_from - 6))),
    ToText   = LTRIM(RTRIM(SUBSTRING(s, pos_to + 4, LEN(s) - pos_to - 3)))
FROM [note] AS t
CROSS APPLY (
    -- Normalize and make the search case-insensitive / accent-insensitive
    SELECT s = LTRIM(RTRIM(ISNULL(t.FromToText, ''))) COLLATE Latin1_General_CI_AI
) AS norm
CROSS APPLY (
    SELECT 
        pos_from = CHARINDEX(' from ', norm.s),
        pos_to   = CHARINDEX(' to ',   norm.s)
) AS pos;