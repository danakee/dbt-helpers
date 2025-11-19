# Install the module if you haven't already
# Install-Module -Name ReportingServicesTools

# Connect to your Report Server
$ReportServerUri = "http://your-pbirs-server/reports"
$reportPortalUri = "http://your-pbirs-server/reports"

# Get all data sources
$dataSources = Get-RsRestItemDataSource -RsItem "/" -Recurse -ReportPortalUri $reportPortalUri

# Export to CSV for auditing
$auditResults = foreach ($ds in $dataSources) {
    [PSCustomObject]@{
        DataSourceName = $ds.Name
        Path = $ds.Path
        ConnectionString = $ds.ConnectionString
        DataSourceType = $ds.Type
        CredentialType = $ds.CredentialRetrieval
        Enabled = $ds.Enabled
    }
}

$auditResults | Export-Csv -Path "C:\Audits\ConnectionStrings.csv" -NoTypeInformation