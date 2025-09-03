-- Split each multi-Automessage note into one row per Automessage
DROP TABLE IF EXISTS #Note4;

;WITH s AS
(
    SELECT
        n.[pkey],
        n.[noteid],
        n.[fk_notetype],
        n.[fk_node],
        n.[subject],
        n.[note],                  -- normalized already in #Note1
        n.[AutomessageCount],
        n.[fk_validtid],
        n.[create_userid],
        n.[create_dt],
        n.[fk_assigntype],
        n.[last_mod_dt],
        n.[last_mod_user],
        n.[latest],
        n.[parent],
        n.[hvr_change_time],
        sp.[value]        AS piece,       -- text between delimiters
        sp.[ordinal]      AS piece_ordinal
    FROM #Note3 AS n
    CROSS APPLY STRING_SPLIT(n.[note], 'Automessage -', 1) AS sp
)
SELECT
    [pkey],
    [noteid],
    [fk_notetype],
    [fk_node],
    [subject],
    -- rebuild a single Automessage note per row
    'Automessage --' + LTRIM(RTRIM(piece)) AS [note],
    1 AS [AutomessageCount],                -- now exactly one per row
    [fk_validtid],
    [create_userid],
    [create_dt],
    [fk_assigntype],
    [last_mod_dt],
    [last_mod_user],
    [latest],
    [parent],
    [hvr_change_time],
    piece_ordinal AS [AutomessageOrdinal]   -- (optional) keep which one it was
INTO #Note4
FROM s
WHERE LTRIM(RTRIM(piece)) <> '';            -- drop the preface before the first delimiter

-- now you can combine the single-Automessage rows with the split ones
SELECT * FROM #Note2
UNION ALL
SELECT
    [pkey],
    [noteid],
    [fk_notetype],
    [fk_node],
    [subject],
    [note],
    [AutomessageCount],
    [fk_validtid],
    [create_userid],
    [create_dt],
    [fk_assigntype],
    [last_mod_dt],
    [last_mod_user],
    [latest],
    [parent],
    [hvr_change_time]
FROM #Note4;
