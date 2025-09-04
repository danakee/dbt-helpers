-- Option 1: Using SUBSTRING and CHARINDEX (works in all SQL Server versions)
SELECT 
    [note],
    SUBSTRING([note], 
        CHARINDEX('FROM ', [note]) + 5, 
        CHARINDEX(' TO ', [note]) - CHARINDEX('FROM ', [note]) - 5
    ) AS [StatusFrom],
    SUBSTRING([note], 
        CHARINDEX(' TO ', [note]) + 4, 
        LEN([note]) - CHARINDEX(' TO ', [note]) - 3
    ) AS [StatusTo]
FROM your_table_name
WHERE [note] LIKE 'AUTOMESSAGE - ISSUE STATUS CHANGED FROM % TO %';

-- Option 2: Using STRING_SPLIT (SQL Server 2016+)
-- This approach splits on ' TO ' and then extracts the parts
WITH SplitData AS (
    SELECT 
        [note],
        SUBSTRING([note], CHARINDEX('FROM ', [note]) + 5, LEN([note])) AS FromToString
    FROM your_table_name
    WHERE [note] LIKE 'AUTOMESSAGE - ISSUE STATUS CHANGED FROM % TO %'
)
SELECT 
    [note],
    SUBSTRING(FromToString, 1, CHARINDEX(' TO ', FromToString) - 1) AS [StatusFrom],
    SUBSTRING(FromToString, CHARINDEX(' TO ', FromToString) + 4, LEN(FromToString)) AS [StatusTo]
FROM SplitData;

-- Option 3: If you want to UPDATE existing columns in your table
UPDATE your_table_name
SET 
    [StatusFrom] = SUBSTRING([note], 
        CHARINDEX('FROM ', [note]) + 5, 
        CHARINDEX(' TO ', [note]) - CHARINDEX('FROM ', [note]) - 5
    ),
    [StatusTo] = SUBSTRING([note], 
        CHARINDEX(' TO ', [note]) + 4, 
        LEN([note]) - CHARINDEX(' TO ', [note]) - 3
    )
WHERE [note] LIKE 'AUTOMESSAGE - ISSUE STATUS CHANGED FROM % TO %';

-- Option 4: If you need to ADD the new columns first
ALTER TABLE your_table_name 
ADD [StatusFrom] NVARCHAR(100), 
    [StatusTo] NVARCHAR(100);

-- Then run the UPDATE statement from Option 3