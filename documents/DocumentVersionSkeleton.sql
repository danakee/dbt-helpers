SELECT
    dh.DOCUMENT_ID,
    dh.DOCUMENT_NAME,
    dv.VERSION,
    dl.FILE_EXTENSION,
    dl.FILE_NAME_WITHOUT_EXTENSION
FROM 
    [PrismFlightSafety_SQL].[dbo].[DOCUMENT_HEADERS] AS dh
    INNER JOIN [PrismFlightSafety_SQL].[dbo].[DOCUMENT_VERSIONS] AS dv
        ON dh.DOCUMENT_ID = dv.DOCUMENT_ID
    INNER JOIN [PrismFlightSafety_SQL].[dbo].[DOCUMENT_LOCATORS] AS dl
        ON dv.DOCUMENT_ID = dl.DOCUMENT_ID
        AND dv.VERSION    = dl.VERSION;