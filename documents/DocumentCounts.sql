-- Unique documents
SELECT 
    COUNT(DISTINCT DOCUMENT_ID) 
FROM 
    [dbo].[DOCUMENT_HEADERS]

-- Unique document + version combinations
SELECT 
    COUNT(*) 
FROM 
    [dbo].[DOCUMENT_VERSIONS]

-- Locators per document/version on average
SELECT 
    AVG(locator_count * 1.0) AS avg_locators_per_version,
    MAX(locator_count)       AS max_locators_per_version,
    MIN(locator_count)       AS min_locators_per_version
FROM (
    SELECT 
        DOCUMENT_ID, VERSION, COUNT(*) AS locator_count
    FROM 
        [dbo].[DOCUMENT_LOCATORS]
    GROUP BY 
        DOCUMENT_ID, VERSION
) x

-- Breakdown by storage type
SELECT 
    st.DESCRIPTION,
    COUNT(*) AS locator_count
FROM 
    [dbo].[DOCUMENT_LOCATORS] dl
    INNER JOIN [dbo].[DOCUMENT_STORAGE_TYPES_EN] st
        ON dl.STORAGE_TYPE = st.STORAGE_TYPE
GROUP BY 
    st.DESCRIPTION
ORDER BY 
    locator_count DESC