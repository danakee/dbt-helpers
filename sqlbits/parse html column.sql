DECLARE @html NVARCHAR(MAX) = '<p>Slated for delivery 8/28/2025</p>    <p>&nbsp;</p>    <table border="1" cellpadding="2">    <tbody>    <tr>    <td style="vertical-align:top">8/27/2025</td>    <td style="vertical-align:top">Sherwin,Tiffany</td>    <td style="vertical-align:top">Automessage - Issue status changed from In-Work to RFR /td>    </tr>    <tr>    <td style="vertical-align:top">8/27/2025</td>    <td style="vertical-align:top">Automessage - Issue assignment changed from Ricks, Laura to Baker, Jacob</td>    </tr>    </tr>    <tr>    <td style="vertical-align:top">8/27/2025</td>    <td style="vertical-align:top">Sherwin,Tiffany</td>    <td style="vertical-align:top">SRO1880 shipping to site via UPS 1Z7301780390165131</td>    <td style="vertical-align:top">8/27/2025</td>    <td style="vertical-align:top">Sherwin,Tiffany</td>    <td style="vertical-align:top">Unit recieved from vendor. Working on shipping documents.</td>    </tr>    </td>    </tr>    <tr>    <td style="vertical-align:top">8/26/2025</td>    <td style="vertical-align:top">Ricks,Laura</td>    <td style="vertical-align:top">UPDATED DUE DATE 9/14/25</td>    </tr>    </tbody>    </table>'

-- Method 1: Using XML parsing (requires cleaning the HTML first)
WITH CleanedHTML AS (
    SELECT 
        -- Clean up the malformed HTML
        REPLACE(
            REPLACE(
                REPLACE(@html, '/td>', '</td>'),
                '</td>    </tr>    </tr>', '</td></tr>'),
            '&nbsp;', ' '
        ) AS cleaned_html
),
ParsedData AS (
    SELECT 
        -- Convert to XML and extract table rows
        CAST('<root>' + cleaned_html + '</root>' AS XML) AS xml_data
    FROM CleanedHTML
)

-- Extract dates and automessage entries
SELECT 
    T.c.value('(td[1])[1]', 'NVARCHAR(50)') AS entry_date,
    T.c.value('(td[2])[1]', 'NVARCHAR(500)') AS person_or_message,
    T.c.value('(td[3])[1]', 'NVARCHAR(500)') AS additional_info,
    CASE 
        WHEN T.c.value('(td[2])[1]', 'NVARCHAR(500)') LIKE '%Automessage - Issue status changed%' 
        THEN T.c.value('(td[2])[1]', 'NVARCHAR(500)')
        WHEN T.c.value('(td[3])[1]', 'NVARCHAR(500)') LIKE '%Automessage - Issue status changed%' 
        THEN T.c.value('(td[3])[1]', 'NVARCHAR(500)')
        ELSE NULL 
    END AS automessage_status_change
FROM ParsedData
CROSS APPLY xml_data.nodes('//tr') AS T(c)
WHERE T.c.value('(td[1])[1]', 'NVARCHAR(50)') IS NOT NULL
    AND (T.c.value('(td[2])[1]', 'NVARCHAR(500)') LIKE '%Automessage - Issue status changed%'
         OR T.c.value('(td[3])[1]', 'NVARCHAR(500)') LIKE '%Automessage - Issue status changed%')