-- Check magic bytes across a sample of UDocumentContent rows
SELECT
    UPPER(CONVERT(VARCHAR(8),
        CAST([dl].[UDOCUMENT_CONTENT] AS VARBINARY(4)),
        2))                                         AS first4_hex,
    [dl].[FILE_EXTENSION]                           AS ext,
    [est].[DESCRIPTION]                             AS storage_type,
    DATALENGTH([dl].[UDOCUMENT_CONTENT])            AS ucontent_len,
    DATALENGTH([dl].[DOCUMENT_CONTENT])             AS content_len,
    CASE WHEN [dl].[DOCUMENT_KEY]  IS NOT NULL
         THEN 'Y' ELSE 'N' END                      AS has_key,
    CASE WHEN [dl].[DOCUMENT_CONTENT] IS NOT NULL
         THEN 'Y' ELSE 'N' END                      AS has_doc_content,
    COUNT(*)                                        AS cnt
FROM 
    [PrismFlightSafety_SQL].[dbo].[DOCUMENT_LOCATORS] AS [dl]
    INNER JOIN [PrismFlightSafety_SQL].[dbo].[DOCUMENT_STORAGE_TYPES_EN] AS [est]
        ON [dl].[STORAGE_TYPE] = [est].[STORAGE_TYPE]
WHERE 
    [dl].[UDOCUMENT_CONTENT] IS NOT NULL
GROUP BY
    UPPER(CONVERT(VARCHAR(8),
        CAST([dl].[UDOCUMENT_CONTENT] AS VARBINARY(4)),
        2)),
    [dl].[FILE_EXTENSION],
    [est].[DESCRIPTION],
    DATALENGTH([dl].[UDOCUMENT_CONTENT]),
    DATALENGTH([dl].[DOCUMENT_CONTENT]),
    CASE WHEN [dl].[DOCUMENT_KEY]  IS NOT NULL
         THEN 'Y' ELSE 'N' END,
    CASE WHEN [dl].[DOCUMENT_CONTENT] IS NOT NULL
         THEN 'Y' ELSE 'N' END
ORDER BY 
    cnt DESC