-- Robust split: "from <FromText> to <ToText>"
SELECT
     [t].[FromToText]
    ,CASE
        WHEN [p].[pos_from] > 0 AND [p].[pos_to] > [p].[pos_from] + 4
        THEN TRIM(SUBSTRING([t].[FromToText],
            [p].[pos_from] + 5,                     -- after "from "
            [p].[pos_to] - ([p].[pos_from] + 5)))       -- up to " to "
        ELSE ''
    END AS [FromText]
    ,CASE
        WHEN [p].[pos_from] > 0 AND [p].[pos_to] > [p].[pos_from] + 4
        THEN TRIM(SUBSTRING([t].[FromToText],
            [p].[pos_to] + 4,                       -- after " to "
            LEN([t].[FromToText]) - ([p].[pos_to] + 3)))
        ELSE ''
    END AS [ToText]
FROM 
    [YourTable] AS [t]
    CROSS APPLY (
        -- Case-insensitive search using LOWER(); positions are applied to the original text
        SELECT
            [pos_from] = CHARINDEX('from ', LOWER([t].[FromToText])),
            [pos_to]   = CHARINDEX(' to ', LOWER([t].[FromToText]))
    ) AS [p];
