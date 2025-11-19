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

$allDataSources = Get-DataSourcesRecursive -Path "/" -Proxy $rs

$auditResults = foreach ($ds in $allDataSources) {
    try {
        $dsContent = $rs.GetDataSourceContents($ds.Path)
        $cs = $dsContent.ConnectString
        
        # Extract server
        $server = if ($cs -match "server\s*=\s*([^;]+)|data\s*source\s*=\s*([^;]+)") {
            ($matches[1] ?? $matches[2]).Trim()
        } else { "Unknown" }
        
        # Extract username
        $username = if ($cs -match "user\s*id\s*=\s*([^;]+)|uid\s*=\s*([^;]+)|user\s*=\s*([^;]+)") {
            ($matches[1] ?? $matches[2] ?? $matches[3]).Trim()
        } elseif ($dsContent.UserName) {
            $dsContent.UserName
        } else { "Integrated/None" }
        
        # Check for password in connection string
        $hasPasswordInCS = $cs -match "password\s*=(?!\s*\*)|pwd\s*=(?!\s*\*)"
        
        # Check for risky usernames
        $riskyUsernames = @('sa', 'admin', 'administrator', 'root', 'dbo', 'sysadmin')
        $isRiskyUsername = $riskyUsernames -contains $username.ToLower()
        
        # Check authentication type
        $authType = if ($cs -match "integrated\s*security\s*=\s*true|trusted_connection\s*=\s*yes" -or 
                        $dsContent.CredentialRetrieval -eq "Integrated") {
            "Windows"
        } elseif ($dsContent.CredentialRetrieval -eq "Store" -or 
                  $cs -match "user\s*id\s*=|uid\s*=") {
            "SQL"
        } elseif ($dsContent.CredentialRetrieval -eq "Prompt") {
            "Prompt"
        } else {
            "None"
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
$report = @"
========================================
POWER BI DATA SOURCE SECURITY AUDIT
Generated: $(Get-Date)
========================================

TOTAL DATA SOURCES: $($auditResults.Count)

CREDENTIAL TYPE BREAKDOWN:
$($auditResults | Group-Object CredentialType | Format-Table Count, Name -AutoSize | Out-String)

AUTHENTICATION TYPE BREAKDOWN:
$($auditResults | Group-Object AuthType | Format-Table Count, Name -AutoSize | Out-String)

HIGH RISK FINDINGS:
-------------------
Passwords in Connection Strings: $($auditResults | Where-Object HasPasswordInCS | Measure-Object | Select-Object -ExpandProperty Count)
Risky Usernames (sa, admin, etc): $($auditResults | Where-Object IsRiskyUsername | Measure-Object | Select-Object -ExpandProperty Count)
Stored Credentials: $($auditResults | Where-Object UsesStoredCreds | Measure-Object | Select-Object -ExpandProperty Count)
SQL Authentication: $($auditResults | Where-Object {$_.AuthType -eq 'SQL'} | Measure-Object | Select-Object -ExpandProperty Count)

RECOMMENDATIONS:
1. Use Windows Authentication (Integrated Security) wherever possible
2. Avoid storing passwords in connection strings
3. Don't use 'sa' or other admin accounts for reporting
4. Use stored credentials only when necessary and rotate regularly
5. Consolidate to service accounts per server to reduce credential sprawl

DETAILED FINDINGS:
"@

$report | Out-File "C:\Audits\SecurityAuditReport.txt"

# Show high-risk items
Write-Host "`n=== HIGH RISK DATA SOURCES ===" -ForegroundColor Red
$highRisk = $auditResults | Where-Object {
    $_.HasPasswordInCS -or $_.IsRiskyUsername -or ($_.UsesStoredCreds -and $_.AuthType -eq "SQL")
}
$highRisk | Format-Table Name, Server, Username, AuthType, HasPasswordInCS, IsRiskyUsername

Write-Host "`nFull audit exported to C:\Audits\ConnectionStrings_Full.csv"
Write-Host "Security report saved to C:\Audits\SecurityAuditReport.txt"