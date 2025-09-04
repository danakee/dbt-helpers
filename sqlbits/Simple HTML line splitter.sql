;WITH [src] AS (
SELECT
    [pkey],
    CAST([original_column] AS nvarchar(max)) AS [html]
FROM 
    [HTMLTable]
),

-- Normalize paragraph breaks to a single delimiter "|"
[norm] AS (
SELECT
    [pkey],
    -- treat <br> as paragraph breaks, collapse any <p ...> to <p>
    REPLACE(
        REPLACE(
            REPLACE(REPLACE(html, '<br/>', '</p>'), '<br>', '</p>'),
        '<p ', '<p>'),
    '</p>', '</p>|') AS [html_norm]
FROM 
    [src]
),
-- Split on the delimiter while keeping original order via ORDINAL
[chunks] AS (
SELECT
    [n].[pkey],
    [s].[ordinal] AS [SeqNum],
    [s].[value] AS [chunk]
FROM 
    [norm] AS [n]
    CROSS APPLY STRING_SPLIT([n].[html_norm], '|', 1) AS [s]
),

-- Strip HTML tags from each chunk using XML (robust without regex)
[clean] AS (
SELECT
    [c].[pkey],
    [c].[SeqNum],
    LTRIM(RTRIM(
        TRY_CONVERT(xml, '<r>' + REPLACE([c].[chunk], '&', '&amp;') + '</r>')
            .value('(/r)[1]', 'nvarchar(max)')
    )) AS [note]
FROM 
    [chunks] AS [c]
)
SELECT
    [pkey],
    [SeqNum],
    [note]
FROM 
    [clean]
WHERE 
    LEN([note]) > 10
ORDER BY 
    [pkey], 
    [SeqNum];
