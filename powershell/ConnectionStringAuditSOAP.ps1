# Using the SOAP Web Service (more reliable for automation)
$reportServerUri = "http://your-pbirs-server/ReportServer/ReportService2010.asmx?wsdl"

# Create proxy with your credentials
$rs = New-WebServiceProxy -Uri $reportServerUri -UseDefaultCredential

# Alternatively, if you need explicit credentials:
# $cred = Get-Credential
# $rs = New-WebServiceProxy -Uri $reportServerUri -Credential $cred

function Get-DataSourcesRecursive {
    param(
        [string]$Path = "/",
        [object]$Proxy
    )
    
    $results = @()
    
    try {
        $items = $Proxy.ListChildren($Path, $false)
        
        foreach ($item in $items) {
            if ($item.TypeName -eq "DataSource") {
                $results += $item
            }
            elseif ($item.TypeName -eq "Folder") {
                $results += Get-DataSourcesRecursive -Path $item.Path -Proxy $Proxy
            }
        }
    }
    catch {
        Write-Warning "Error accessing $Path : $_"
    }
    
    return $results
}

Write-Host "Scanning for data sources..." -ForegroundColor Cyan
$allDataSources = Get-DataSourcesRecursive -Path "/" -Proxy $rs

Write-Host "Found $($allDataSources.Count) data sources. Retrieving connection strings..." -ForegroundColor Cyan

$auditResults = foreach ($ds in $allDataSources) {
    Write-Host "Processing: $($ds.Path)" -ForegroundColor Gray
    
    try {
        $dsContent = $rs.GetDataSourceContents($ds.Path)
        
        [PSCustomObject]@{
            Name = $ds.Name
            Path = $ds.Path
            ConnectionString = $dsContent.ConnectString
            Provider = $dsContent.Extension
            CredentialType = $dsContent.CredentialRetrieval
            Username = $dsContent.UserName
            UseOriginalConnectString = $dsContent.OriginalConnectStringExpressionBased
            Enabled = $dsContent.Enabled
            ModifiedBy = $ds.ModifiedBy
            ModifiedDate = $ds.ModifiedDate
        }
    }
    catch {
        Write-Warning "Could not retrieve details for $($ds.Path): $_"
    }
}

# Export results
New-Item -Path "C:\Audits" -ItemType Directory -Force | Out-Null
$auditResults | Export-Csv -Path "C:\Audits\ConnectionStrings.csv" -NoTypeInformation
Write-Host "`nExported $($auditResults.Count) data sources to C:\Audits\ConnectionStrings.csv" -ForegroundColor Green

# Display summary
$auditResults | Format-Table Name, Provider, CredentialType -AutoSize