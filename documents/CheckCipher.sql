-- 1. Are all DOCUMENT_KEY values the same length?
--    If so, they're likely tokens/identifiers not actual keys
SELECT
    DATALENGTH(DOCUMENT_KEY)    AS key_length,
    COUNT(*)                    AS cnt
FROM [dbo].[DOCUMENT_LOCATORS]
WHERE DOCUMENT_KEY IS NOT NULL
GROUP BY DATALENGTH(DOCUMENT_KEY)
ORDER BY cnt DESC

-- 2. How many distinct key values are there?
--    A small number of distinct keys = application-level keys
--    A unique key per document = document-level keys
SELECT
    COUNT(*)                        AS total_rows,
    COUNT(DISTINCT DOCUMENT_KEY)    AS distinct_keys
FROM [dbo].[DOCUMENT_LOCATORS]
WHERE DOCUMENT_KEY IS NOT NULL

-- 3. Sample the actual key values — do they look like
--    random tokens or human-readable passphrases?
SELECT DISTINCT TOP 20
    LEFT(DOCUMENT_KEY, 50)          AS key_sample,
    DATALENGTH(DOCUMENT_KEY)        AS key_len,
    COUNT(*) OVER (
        PARTITION BY DOCUMENT_KEY)  AS docs_using_this_key
FROM [dbo].[DOCUMENT_LOCATORS]
WHERE DOCUMENT_KEY IS NOT NULL
ORDER BY docs_using_this_key DESC