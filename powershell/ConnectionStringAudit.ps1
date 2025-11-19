$reportPortalUri = "http://your-pbirs-server/reports"

function Get-AllDataSourcesRecursive {
    param(
        [string]$Path = "/",
        [string]$ReportPortalUri
    )
    
    $results = @()
    
    try {
        # Get all items in current path
        $items = Get-RsRestItem -ReportPortalUri $ReportPortalUri -RsItem $Path
        
        foreach ($item in $items) {
            if ($item.Type -eq "DataSource") {
                # Found a data source - add it to results
                $results += $item
            }
            elseif ($item.Type -eq "Folder") {
                # Recurse into folder
                $results += Get-AllDataSourcesRecursive -Path $item.Path -ReportPortalUri $ReportPortalUri
            }
        }
    }
    catch {
        Write-Warning "Error accessing path $Path : $_"
    }
    
    return $results
}

# Get all data sources recursively
Write-Host "Scanning for data sources..." -ForegroundColor Cyan
$allDataSources = Get-AllDataSourcesRecursive -Path "/" -ReportPortalUri $reportPortalUri

Write-Host "Found $($allDataSources.Count) data sources. Retrieving connection strings..." -ForegroundColor Cyan

# Get connection details for each
$auditResults = foreach ($ds in $allDataSources) {
    Write-Host "Processing: $($ds.Path)" -ForegroundColor Gray
    
    try {
        $dsDetails = Get-RsRestItemDataSource -RsItem $ds.Path -ReportPortalUri $reportPortalUri
        
        [PSCustomObject]@{
            Name = $ds.Name
            Path = $ds.Path
            ConnectionString = $dsDetails.ConnectionString
            DataSourceType = if ($dsDetails.DataModelDataSource) { $dsDetails.DataModelDataSource.Kind } else { "Unknown" }
            CredentialType = $dsDetails.CredentialRetrieval
            Enabled = $dsDetails.Enabled
            ModifiedBy = $ds.ModifiedBy
            ModifiedDate = $ds.ModifiedDate
        }
    }
    catch {
        Write-Warning "Could not retrieve details for $($ds.Path): $_"
        
        [PSCustomObject]@{
            Name = $ds.Name
            Path = $ds.Path
            ConnectionString = "ERROR: $_"
            DataSourceType = "ERROR"
            CredentialType = "ERROR"
            Enabled = $null
            ModifiedBy = $ds.ModifiedBy
            ModifiedDate = $ds.ModifiedDate
        }
    }
}

# Export and display results
$auditResults | Export-Csv -Path "C:\Audits\ConnectionStrings.csv" -NoTypeInformation
Write-Host "`nExported to C:\Audits\ConnectionStrings.csv" -ForegroundColor Green
$auditResults | Format-Table -AutoSize