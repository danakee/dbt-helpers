WITH CleanedHTML AS (
    SELECT 
        [pkey],
        [note] as original_html,
        -- Clean up common HTML entities and malformed tags
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE([note], '&nbsp;', ' '),
                    '&amp;', '&'
                ),
                '&lt;', '<'
            ),
            '&gt;', '>'
        ) AS cleaned_html
    FROM [Fsi_Issues2].[dbo].[tblNotes]
    WHERE [pkey] = 10040598
),
ExtractedData AS (
    SELECT 
        [pkey],
        original_html,
        cleaned_html,
        -- Find all positions of "Automessage - Issue status changed"
        LEN(cleaned_html) - LEN(REPLACE(cleaned_html, 'Automessage - Issue status changed', '')) / LEN('Automessage - Issue status changed') as automessage_count
    FROM CleanedHTML
)
SELECT 
    [pkey],
    -- Extract dates using pattern matching
    CASE 
        WHEN PATINDEX('%[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]%', cleaned_html) > 0
        THEN SUBSTRING(
            cleaned_html, 
            PATINDEX('%[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]%', cleaned_html),
            10
        )
    END AS first_date,
    -- Extract automessage content
    CASE 
        WHEN CHARINDEX('Automessage - Issue status changed', cleaned_html) > 0
        THEN SUBSTRING(
            cleaned_html,
            CHARINDEX('Automessage - Issue status changed', cleaned_html),
            CASE 
                WHEN CHARINDEX('</td>', cleaned_html, CHARINDEX('Automessage - Issue status changed', cleaned_html)) > 0
                THEN CHARINDEX('</td>', cleaned_html, CHARINDEX('Automessage - Issue status changed', cleaned_html)) - CHARINDEX('Automessage - Issue status changed', cleaned_html)
                ELSE 200 -- fallback length
            END
        )
    END AS automessage_content,
    automessage_count
FROM ExtractedData