SELECT
    [dl].[DOCUMENT_ID],
    [dl].[VERSION],
    [dl].[DOCUMENT_NUMBER],
    [dl].[STORAGE_TYPE],
    [dh].[DOCUMENT_NAME],
    [dv].[APPROVAL_STATUS]
FROM 
    [PrismFlightSafety_SQL].[dbo].[DOCUMENT_LOCATORS] AS [dl]
    INNER JOIN [PrismFlightSafety_SQL].[dbo].[DOCUMENT_HEADERS] AS [dh]
        ON [dl].[DOCUMENT_ID] = [dh].[DOCUMENT_ID]
    INNER JOIN [PrismFlightSafety_SQL].[dbo].[DOCUMENT_VERSIONS] AS [dv]
        ON [dl].[DOCUMENT_ID] = [dv].[DOCUMENT_ID]
        AND [dl].[VERSION]    = [dv].[VERSION]
WHERE 
    1=1
    AND [dl].[DOCUMENT_LINK]    IS NULL
    AND [dl].[DOCUMENT_FILE]    IS NULL
    AND [dl].[DOCUMENT_CONTENT] IS NULL
    AND [dl].[DOCUMENT_KEY]     IS NULL