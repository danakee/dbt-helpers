-- Just grab 10 random rows and look at the raw bytes
SELECT TOP 10
    [dl].[DOCUMENT_ID],
    [dl].[FILE_EXTENSION]                               AS ext,
    DATALENGTH([dl].[UDOCUMENT_CONTENT])                AS ucontent_len,
    UPPER(CONVERT(VARCHAR(8),
        CAST([dl].[UDOCUMENT_CONTENT] AS VARBINARY(4)),
        2))                                             AS ucontent_first4_hex,
    UPPER(CONVERT(VARCHAR(64),
        CAST([dl].[UDOCUMENT_CONTENT] AS VARBINARY(32)),
        2))                                             AS ucontent_first32_hex,
    CASE WHEN [dl].[DOCUMENT_KEY] IS NOT NULL
         THEN 'Y' ELSE 'N' END                          AS has_key,
    CASE WHEN [dl].[DOCUMENT_CONTENT] IS NOT NULL
         THEN 'Y' ELSE 'N' END                          AS has_doc_content
FROM 
    [PrismFlightSafety_SQL].[dbo].[DOCUMENT_LOCATORS] AS [dl]
WHERE 
    [dl].[UDOCUMENT_CONTENT] IS NOT NULL
    AND [dl].[FILE_EXTENSION] = 'pdf'   -- start with PDFs since %PDF magic is unmistakable
ORDER BY 
    NEWID()