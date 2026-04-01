-- How many rows have UDocumentContent populated?
SELECT
    CASE WHEN [UDOCUMENT_CONTENT] IS NOT NULL THEN 1 ELSE 0 END AS has_ucontent,
    CASE WHEN [DOCUMENT_CONTENT]  IS NOT NULL THEN 1 ELSE 0 END AS has_content,
    CASE WHEN [DOCUMENT_KEY]      IS NOT NULL THEN 1 ELSE 0 END AS has_key,
    CASE WHEN [DOCUMENT_FILE]     IS NOT NULL THEN 1 ELSE 0 END AS has_file,
    CASE WHEN [DOCUMENT_LINK]     IS NOT NULL THEN 1 ELSE 0 END AS has_link,
    COUNT(*)                                                     AS cnt
FROM 
    [PrismFlightSafety_SQL].[dbo].[DOCUMENT_LOCATORS]
WHERE 
    [UDOCUMENT_CONTENT] IS NOT NULL
GROUP BY
    CASE WHEN [UDOCUMENT_CONTENT] IS NOT NULL THEN 1 ELSE 0 END,
    CASE WHEN [DOCUMENT_CONTENT]  IS NOT NULL THEN 1 ELSE 0 END,
    CASE WHEN [DOCUMENT_KEY]      IS NOT NULL THEN 1 ELSE 0 END,
    CASE WHEN [DOCUMENT_FILE]     IS NOT NULL THEN 1 ELSE 0 END,
    CASE WHEN [DOCUMENT_LINK]     IS NOT NULL THEN 1 ELSE 0 END
ORDER BY 
    cnt DESC

-- What storage types and file types are they?
SELECT
    [est].[DESCRIPTION]             AS StorageTypeDescription,
    [dl].[FILE_EXTENSION]           AS FileExtension,
    DATALENGTH([dl].[UDOCUMENT_CONTENT]) AS UContentSize,
    DATALENGTH([dl].[DOCUMENT_CONTENT])  AS ContentSize,
    CASE 
        WHEN [dl].[DOCUMENT_KEY] IS NOT NULL
            THEN LEFT([dl].[DOCUMENT_KEY], 10)
        ELSE NULL 
    END              AS KeyPrefix,
    COUNT(*)                        AS cnt
FROM 
    [PrismFlightSafety_SQL].[dbo].[DOCUMENT_LOCATORS] AS [dl]
    INNER JOIN [PrismFlightSafety_SQL].[dbo].[DOCUMENT_STORAGE_TYPES_EN] AS [est]
        ON [dl].[STORAGE_TYPE] = [est].[STORAGE_TYPE]
WHERE 
    [dl].[UDOCUMENT_CONTENT] IS NOT NULL
GROUP BY
    [est].[DESCRIPTION],
    [dl].[FILE_EXTENSION],
    DATALENGTH([dl].[UDOCUMENT_CONTENT]),
    DATALENGTH([dl].[DOCUMENT_CONTENT]),
    CASE 
        WHEN [dl].[DOCUMENT_KEY] IS NOT NULL
            THEN LEFT([dl].[DOCUMENT_KEY], 10)
        ELSE NULL 
    END
ORDER BY cnt DESC

-- Sample a row to inspect the raw UDocumentContent bytes
SELECT TOP 1
    [dl].[DOCUMENT_ID],
    [dl].[VERSION],
    [dl].[FILE_EXTENSION],
    DATALENGTH([dl].[UDOCUMENT_CONTENT])        AS ucontent_len,
    DATALENGTH([dl].[DOCUMENT_CONTENT])         AS content_len,
    CAST([dl].[UDOCUMENT_CONTENT] AS VARBINARY(32)) AS ucontent_first32,
    CAST([dl].[DOCUMENT_CONTENT]  AS VARBINARY(16)) AS content_first16,
    LEFT([dl].[DOCUMENT_KEY], 20)               AS key_prefix
FROM 
    [PrismFlightSafety_SQL].[dbo].[DOCUMENT_LOCATORS] AS [dl]
WHERE 
    [dl].[UDOCUMENT_CONTENT] IS NOT NULL
    AND [dl].[DOCUMENT_KEY]      IS NOT NULL
ORDER BY 
    DATALENGTH([dl].[UDOCUMENT_CONTENT]) DESC