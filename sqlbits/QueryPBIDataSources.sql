SELECT
    c.Path          AS ItemPath,
    c.Type          AS ItemType,
    ds.Name         AS DataSourceName,
    ds.Extension,
    ds.ConnectString,
    ds.CredentialRetrieval
FROM 
    dbo.DataSource ds
    JOIN dbo.Catalog c
        ON ds.ItemID = c.ItemID
    LEFT JOIN dbo.Catalog sds
        ON ds.Link = sds.ItemID;  -- shared data source reference
