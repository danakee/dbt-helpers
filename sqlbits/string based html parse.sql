-- Method 2: String manipulation approach
DECLARE @html NVARCHAR(MAX) = 'your_html_here'

SELECT 
    SUBSTRING(@html, 
        CHARINDEX('>', @html, pos.start_pos) + 1,
        CHARINDEX('<', @html, CHARINDEX('>', @html, pos.start_pos) + 1) - CHARINDEX('>', @html, pos.start_pos) - 1
    ) AS extracted_date,
    SUBSTRING(@html, 
        CHARINDEX('Automessage - Issue status changed', @html, pos.start_pos),
        CHARINDEX('</td>', @html, CHARINDEX('Automessage - Issue status changed', @html, pos.start_pos)) - 
        CHARINDEX('Automessage - Issue status changed', @html, pos.start_pos)
    ) AS automessage_info
FROM (
    SELECT CHARINDEX('Automessage - Issue status changed', @html) AS start_pos
) pos
WHERE pos.start_pos > 0