-- Split multi-Automessage notes in #Note3 into one row per Automessage
DROP TABLE IF EXISTS [#Note4];

;WITH [parts] AS ( 
SELECT 
     [n].[pkey] 
    ,[n].[noteid] 
    ,[n].[fk_notetype] 
    ,[n].[fk_node] 
    ,[n].[subject] 
    ,[n].[AutomessageCount] 
    ,[n].[fk_vaultid] 
    ,[n].[create_userid] 
    ,[n].[create_dt] 
    ,[n].[fk_assigntype] 
    ,[n].[last_mod_dt] 
    ,[n].[last_mod_user] 
    ,[n].[latest] 
    ,[n].[parent] 
    ,[n].[hvr_change_time] 
    ,[j].[value]  AS [piece]       -- one split chunk (JSON already unescaped) 
    ,[j].[key]    AS [piece_order] 
    ,CASE  
         WHEN LEFT([n].[note], LEN('Automessage -')) = 'Automessage -'  
              THEN 1 ELSE 0  
    END AS [starts_with_delim] 
FROM 
    [#Note3] AS [n] 
    CROSS APPLY OPENJSON( 
        '["' + REPLACE(STRING_ESCAPE([n].[note], 'json'), 'Automessage -', '","') + '"]'
) AS [j]
)
SELECT
      [pkey]
    , [noteid]
    , [fk_notetype]
    , [fk_node]
    , [subject]
    , 'Automessage -' + LTRIM(RTRIM([piece])) AS [note]   -- normalize each row
    , 1 AS [AutomessageCount]
    , [fk_vaultid]
    , [create_userid]
    , [create_dt]
    , [fk_assigntype]
    , [last_mod_dt]
    , [last_mod_user]
    , [latest]
    , [parent]
    , [hvr_change_time]
    , [piece_order] AS [AutomessageOrdinal]   -- optional
INTO 
    [#Note4]
FROM 
    [parts]
WHERE
    ([starts_with_delim] = 1 AND LTRIM(RTRIM([piece])) <> '')
    OR
    ([starts_with_delim] = 0 AND [piece_order] > 0);

-- Combine single- and multi-Automessage rows
SELECT *
FROM [#Note2]
UNION ALL
SELECT
      [pkey]
    , [noteid]
    , [fk_notetype]
    , [fk_node]
    , [subject]
    , [note]
    , [AutomessageCount]
    , [fk_vaultid]
    , [create_userid]
    , [create_dt]
    , [fk_assigntype]
    , [last_mod_dt]
    , [last_mod_user]
    , [latest]
    , [parent]
    , [hvr_change_time]
FROM 
    [#Note4];
