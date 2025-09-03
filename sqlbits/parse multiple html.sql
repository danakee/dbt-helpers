WITH CleanedHTML AS (
    SELECT 
        [pkey],
        [note] as original_html,
        REPLACE(
            REPLACE(
                REPLACE([note], '&nbsp;', ' '),
                '&amp;', '&'
            ),
            CHAR(13) + CHAR(10), ' '
        ) AS cleaned_html
    FROM [Fsi_Issues2].[dbo].[tblNotes]
    WHERE [pkey] = 10040598
),
AutomessageExtractor AS (
    -- Anchor: Find first automessage
    SELECT 
        [pkey],
        cleaned_html,
        1 as occurrence_number,
        CHARINDEX('Automessage - Issue status changed', cleaned_html) as start_pos,
        CASE 
            WHEN CHARINDEX('Automessage - Issue status changed', cleaned_html) > 0
            THEN SUBSTRING(
                cleaned_html,
                CHARINDEX('Automessage - Issue status changed', cleaned_html),
                CASE 
                    WHEN CHARINDEX('</td>', cleaned_html, CHARINDEX('Automessage - Issue status changed', cleaned_html)) > 0
                    THEN CHARINDEX('</td>', cleaned_html, CHARINDEX('Automessage - Issue status changed', cleaned_html)) - CHARINDEX('Automessage - Issue status changed', cleaned_html)
                    ELSE 200
                END
            )
        END as automessage_text
    FROM CleanedHTML
    WHERE CHARINDEX('Automessage - Issue status changed', cleaned_html) > 0
    
    UNION ALL
    
    -- Recursive: Find subsequent automessages
    SELECT 
        a.[pkey],
        a.cleaned_html,
        a.occurrence_number + 1,
        CHARINDEX('Automessage - Issue status changed', a.cleaned_html, a.start_pos + 1) as start_pos,
        CASE 
            WHEN CHARINDEX('Automessage - Issue status changed', a.cleaned_html, a.start_pos + 1) > 0
            THEN SUBSTRING(
                a.cleaned_html,
                CHARINDEX('Automessage - Issue status changed', a.cleaned_html, a.start_pos + 1),
                CASE 
                    WHEN CHARINDEX('</td>', a.cleaned_html, CHARINDEX('Automessage - Issue status changed', a.cleaned_html, a.start_pos + 1)) > 0
                    THEN CHARINDEX('</td>', a.cleaned_html, CHARINDEX('Automessage - Issue status changed', a.cleaned_html, a.start_pos + 1)) - CHARINDEX('Automessage - Issue status changed', a.cleaned_html, a.start_pos + 1)
                    ELSE 200
                END
            )
        END as automessage_text
    FROM AutomessageExtractor a
    WHERE CHARINDEX('Automessage - Issue status changed', a.cleaned_html, a.start_pos + 1) > 0
        AND a.occurrence_number < 10 -- Prevent infinite recursion
)
SELECT 
    [pkey],
    occurrence_number,
    automessage_text,
    -- Extract date from preceding content if possible
    CASE 
        WHEN start_pos > 50 
        THEN SUBSTRING(cleaned_html, start_pos - 50, 50)
    END as preceding_context
FROM AutomessageExtractor
ORDER BY [pkey], occurrence_number