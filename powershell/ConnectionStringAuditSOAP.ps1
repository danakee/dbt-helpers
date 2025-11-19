$reportServerUri = "http://your-server/reportserver/ReportService2010.asmx?wsdl"
$rs = New-WebServiceProxy -Uri $reportServerUri -UseDefaultCredential

# Get all data sources
$items = $rs.ListChildren("/", $true) | Where-Object {$_.TypeName -eq "DataSource"}

$auditResults = foreach ($item in $items) {
    $ds = $rs.GetDataSourceContents($item.Path)
    [PSCustomObject]@{
        Path = $item.Path
        Name = $item.Name
        ConnectionString = $ds.ConnectString
        Provider = $ds.Extension
        CredentialType = $ds.CredentialRetrieval
    }
}

$auditResults | Export-Csv -Path "C:\Audits\ConnectionStrings.csv" -NoTypeInformation