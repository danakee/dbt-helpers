$reportServerUri = "http://your-pbirs-server/ReportServer/ReportService2010.asmx?wsdl"
$rs = New-WebServiceProxy -Uri $reportServerUri -UseDefaultCredential

function Get-DataSourcesRecursive {
    param([string]$Path = "/", [object]$Proxy)
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
    catch { Write-Warning "Error accessing $Path : $_" }
    return $results
}

Write-Host "Scanning for data sources..." -ForegroundColor Cyan
$allDataSources = Get-DataSourcesRecursive -Path "/" -Proxy $rs

Write-Host "Found $($allDataSources.Count) data sources. Retrieving connection strings..." -ForegroundColor Cyan

$auditResults = foreach ($ds in $allDataSources) {
    Write-Host "Processing: $($ds.Path)" -ForegroundColor Gray
    
    try {
        $dsContent = $rs.GetDataSourceContents($ds.Path)
        $cs = $dsContent.ConnectString
        
        # Extract server
        $server = "Unknown"
        if ($cs -match "server\s*=\s*([^;]+)") {
            $server = $matches[1].Trim()
        }
        elseif ($cs -match "data\s*source\s*=\s*([^;]+)") {
            $server = $matches[1].Trim()
        }
        
        # Extract username
        $username = "Integrated/None"
        if ($cs -match "user\s*id\s*=\s*([^;]+)") {
            $username = $matches[1].Trim()
        }
        elseif ($cs -match "uid\s*=\s*([^;]+)") {
            $username = $matches[1].Trim()
        }
        elseif ($cs -match "user\s*=\s*([^;]+)") {
            $username = $matches[1].Trim()
        }
        elseif ($dsContent.UserName) {
            $username = $dsContent.UserName
        }
        
        # Check for password in connection string
        $hasPasswordInCS = $cs -match "password\s*=(?!\s*\*)" -or $cs -match "pwd\s*=(?!\s*\*)"
        
        # Check for risky usernames
        $riskyUsernames = @('sa', 'admin', 'administrator', 'root', 'dbo', 'sysadmin')
        $isRiskyUsername = $riskyUsernames -contains $username.ToLower()
        
        # Check authentication type
        $authType = "None"
        if ($cs -match "integrated\s*security\s*=\s*true" -or 
            $cs -match "trusted_connection\s*=\s*yes" -or 
            $dsContent.CredentialRetrieval -eq "Integrated") {
            $authType = "Windows"
        }
        elseif ($dsContent.CredentialRetrieval -eq "Store" -or 
                $cs -match "user\s*id\s*=" -or 
                $cs -match "uid\s*=") {
            $authType = "SQL"
        }
        elseif ($dsContent.CredentialRetrieval -eq "Prompt") {
            $authType = "Prompt"
        }
        
        [PSCustomObject]@{
            Name = $ds.Name
            Path = $ds.Path
            Server = $server
            Username = $username
            ConnectionString = $cs
            Provider = $dsContent.Extension
            CredentialType = $dsContent.CredentialRetrieval
            AuthType = $authType
            HasPasswordInCS = $hasPasswordInCS
            IsRiskyUsername = $isRiskyUsername
            UsesStoredCreds = ($dsContent.CredentialRetrieval -eq "Store")
            Enabled = $dsContent.Enabled
            ModifiedBy = $ds.ModifiedBy
            ModifiedDate = $ds.ModifiedDate
        }
    }
    catch {
        Write-Warning "Could not retrieve details for $($ds.Path): $_"
    }
}

# Export full audit
New-Item -Path "C:\Audits" -ItemType Directory -Force | Out-Null
$auditResults | Export-Csv -Path "C:\Audits\ConnectionStrings_Full.csv" -NoTypeInformation

# Generate compliance report
$totalCount = $auditResults.Count
$credTypeBreakdown = $auditResults | Group-Object CredentialType | ForEach-Object { "  $($_.Name): $($_.Count)" }
$authTypeBreakdown = $auditResults | Group-Object AuthType | ForEach-Object { "  $($_.Name): $($_.Count)" }
$passwordInCS = ($auditResults | Where-Object HasPasswordInCS | Measure-Object).Count
$riskyUsers = ($auditResults | Where-Object IsRiskyUsername | Measure-Object).Count
$storedCreds = ($auditResults | Where-Object UsesStoredCreds | Measure-Object).Count
$sqlAuth = ($auditResults | Where-Object {$_.AuthType -eq 'SQL'} | Measure-Object).Count

$report = @"
========================================
POWER BI DATA SOURCE SECURITY AUDIT
Generated: $(Get-Date)
========================================

TOTAL DATA SOURCES: $totalCount

CREDENTIAL TYPE BREAKDOWN:
$($credTypeBreakdown -join "`n")

AUTHENTICATION TYPE BREAKDOWN:
$($authTypeBreakdown -join "`n")

HIGH RISK FINDINGS:
-------------------
Passwords in Connection Strings: $passwordInCS
Risky Usernames (sa, admin, etc): $riskyUsers
Stored Credentials: $storedCreds
SQL Authentication: $sqlAuth

RECOMMENDATIONS:
1. Use Windows Authentication (Integrated Security) wherever possible
2. Avoid storing passwords in connection strings
3. Don't use 'sa' or other admin accounts for reporting
4. Use stored credentials only when necessary and rotate regularly
5. Consolidate to service accounts per server to reduce credential sprawl

========================================
"@

$report | Out-File "C:\Audits\SecurityAuditReport.txt"

# Show high-risk items
Write-Host "`n=== HIGH RISK DATA SOURCES ===" -ForegroundColor Red
$highRisk = $auditResults | Where-Object {
    $_.HasPasswordInCS -or $_.IsRiskyUsername -or ($_.UsesStoredCreds -and $_.AuthType -eq "SQL")
}

if ($highRisk) {
    $highRisk | Format-Table Name, Server, Username, AuthType, HasPasswordInCS, IsRiskyUsername -AutoSize
}
else {
    Write-Host "No high-risk data sources found!" -ForegroundColor Green
}

# Summary
Write-Host "`n=== SUMMARY ===" -ForegroundColor Cyan
Write-Host "Total Data Sources: $totalCount"
Write-Host "High Risk Items: $($highRisk.Count)" -ForegroundColor $(if ($highRisk.Count -gt 0) { "Red" } else { "Green" })
Write-Host "SQL Authentication: $sqlAuth" -ForegroundColor $(if ($sqlAuth -gt 0) { "Yellow" } else { "Green" })
Write-Host "Windows Authentication: $(($auditResults | Where-Object {$_.AuthType -eq 'Windows'} | Measure-Object).Count)" -ForegroundColor Green

Write-Host "`nFull audit exported to C:\Audits\ConnectionStrings_Full.csv" -ForegroundColor Cyan
Write-Host "Security report saved to C:\Audits\SecurityAuditReport.txt" -ForegroundColor Cyan
