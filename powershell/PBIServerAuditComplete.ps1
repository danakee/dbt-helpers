param(
    [string]$ReportServerUri = "https://YOURSERVER/ReportServer/ReportService2010.asmx?wsdl",
    [string]$AuditOutputPath = "C:\scripts\PBIAudit"
)

<# =====================================================================
 POWER BI REPORT SERVER – COMBINED SECURITY AUDIT
 -----------------------------------------------------------------------
 This script connects to a Power BI / SSRS Report Server instance,
 and runs TWO audits in a single pass:

   1. Report Security Audit (RDL analysis)
   2. Data Source / Connection String Security Audit

 Outputs (in $AuditOutputPath):

   - PBIReportSecurityAudit.csv     (per-report details)
   - PBIConnectionStrings.csv      (per-data-source details)
   - PBICombinedSecurityReport.txt (combined narrative summary)

 Notes:
   * Run in Windows PowerShell 5.x
   * First, unblock this script if necessary:
       Unblock-File -Path .\PBIServerCombinedAudit.ps1

   * Override defaults:
       .\PBIServerCombinedAudit.ps1 `
         -ReportServerUri "https://myServer/ReportServer/ReportService2010.asmx?wsdl" `
         -AuditOutputPath "D:\PBIAudit"
 ===================================================================== #>

# Parse server name (host) from URI
$serverUri        = [System.Uri]$ReportServerUri
$reportServerName = $serverUri.Host

Write-Host "Target Report Server: $reportServerName" -ForegroundColor Cyan
Write-Host ""

# Create proxy once – reused by both audits
$rs = New-WebServiceProxy -Uri $ReportServerUri -UseDefaultCredential

# Ensure output directory exists
New-Item -Path $AuditOutputPath -ItemType Directory -Force | Out-Null


# =====================================================================
#  HELPER FUNCTIONS
# =====================================================================

function Get-ReportsRecursive {
    param (
        [string]$Path = "/",
        [object]$Proxy
    )

    $results = @()

    try {
        $items = $Proxy.ListChildren($Path, $false)

        foreach ($item in $items) {
            if ($item.TypeName -eq "Report") {
                $results += $item
            }
            elseif ($item.TypeName -eq "Folder") {
                $results += Get-ReportsRecursive -Path $item.Path -Proxy $Proxy
            }
        }
    }
    catch {
        Write-Warning "Error accessing $Path : $_"
    }

    return $results
}

function Get-DataSourcesRecursive {
    param (
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


# =====================================================================
#  SECTION 1 – REPORT SECURITY AUDIT (RDL ANALYSIS)
# =====================================================================

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "POWER BI REPORT SECURITY AUDIT (RDL)" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Scanning for reports..." -ForegroundColor Cyan
$allReports = Get-ReportsRecursive -Path "/" -Proxy $rs
Write-Host "Found $($allReports.Count) reports to analyze on $reportServerName" -ForegroundColor Green
Write-Host ""

$reportAudit = foreach ($report in $allReports) {

    Write-Host "Analyzing: $($report.Path)" -ForegroundColor Gray

    # Initialize all variables with defaults
    $hasEmbeddedDataSources = $false
    $embeddedDSCount        = 0
    $hasEmbeddedCreds       = $false
    $credentialTypes        = @()
    $hasCustomCode          = $false
    $hasExternalImages      = $false
    $externalImageCount     = 0
    $hasParameters          = $false
    $parameterCount         = 0
    $hasUnsafeExpressions   = $false
    $hasSubreports          = $false
    $subreportCount         = 0

    try {
        # Get report definition
        $reportDef = $rs.GetItemDefinition($report.Path)
        $rdlText   = [System.Text.Encoding]::UTF8.GetString($reportDef)

        # Clean the XML - remove BOM and problematic characters
        $rdlText = $rdlText.Trim()
        if ($rdlText.StartsWith([char]0xFEFF)) {
            $rdlText = $rdlText.Substring(1)
        }

        # Parse XML with better error handling
        try {
            [xml]$rdlXml = $rdlText
        }
        catch {
            Write-Warning "XML parse error for $($report.Path): $_"

            # Skip this report if XML won't parse – but still output a row
            [PSCustomObject]@{
                ReportServerName        = $reportServerName
                ReportName              = $report.Name
                ReportPath              = $report.Path
                HasEmbeddedDataSources  = $false
                EmbeddedDataSourceCount = 0
                HasEmbeddedCredentials  = $false
                EmbeddedCredTypes       = "XML_PARSE_ERROR"
                HasCustomCode           = $false
                HasExternalImages       = $false
                ExternalImageCount      = 0
                HasParameters           = $false
                ParameterCount          = 0
                HasPotentialInjection   = $false
                HasSubreports           = $false
                SubreportCount          = 0
                ModifiedBy              = $report.ModifiedBy
                ModifiedDate            = $report.ModifiedDate
            }

            continue
        }

        # -------------------------------------------------------------
        # Check for data sources
        # -------------------------------------------------------------
        if ($rdlXml.Report.DataSources) {
            $allDataSources = $rdlXml.Report.DataSources.DataSource

            if ($allDataSources) {
                # Ensure it's an array
                if ($allDataSources -isnot [System.Array]) {
                    $allDataSources = @($allDataSources)
                }

                # Check each data source
                foreach ($ds in $allDataSources) {

                    # Embedded = has ConnectionProperties
                    if ($ds.ConnectionProperties) {
                        $hasEmbeddedDataSources = $true
                        $embeddedDSCount++

                        $connString = $ds.ConnectionProperties.ConnectString
                        if ($connString) {
                            # Check for password in connection string
                            if ($connString -match "password\s*=" -or
                                $connString -match "pwd\s*=") {
                                $hasEmbeddedCreds = $true
                            }

                            # Determine credential type
                            if ($ds.ConnectionProperties.IntegratedSecurity -eq "true") {
                                $credentialTypes += "Integrated"
                            }
                            elseif ($ds.ConnectionProperties.Prompt) {
                                $credentialTypes += "Prompt"
                            }
                            else {
                                $credentialTypes += "Stored/Embedded"
                            }
                        }
                    }

                    # If it has DataSourceReference, it's a shared data source (GOOD)
                }
            }
        }

        # -------------------------------------------------------------
        # Check for custom code
        # -------------------------------------------------------------
        if ($rdlXml.Report.Code) {
            $codeContent = $rdlXml.Report.Code.Trim()
            if ($codeContent -ne "") {
                $hasCustomCode = $true
            }
        }

        # -------------------------------------------------------------
        # Check for external images
        # -------------------------------------------------------------
        $externalImageNodes = $rdlXml.SelectNodes("//Image[@Source='External']")
        if ($externalImageNodes) {
            $externalImageCount = $externalImageNodes.Count
            $hasExternalImages  = $externalImageCount -gt 0
        }

        # -------------------------------------------------------------
        # Check for parameters
        # -------------------------------------------------------------
        if ($rdlXml.Report.ReportParameters.ReportParameter) {
            $params = $rdlXml.Report.ReportParameters.ReportParameter

            if ($params -isnot [System.Array]) {
                $params = @($params)
            }

            $parameterCount = $params.Count
            $hasParameters  = $parameterCount -gt 0
        }

        # -------------------------------------------------------------
        # Check for unsafe parameter expressions (SQL injection risk)
        # -------------------------------------------------------------
        $rdlContent = $rdlXml.InnerXml
        if ($rdlContent -match 'Parameters!.*\+\s*"' -or
            $rdlContent -match '=Parameters!.*&amp;\s*"') {
            $hasUnsafeExpressions = $true
        }

        # -------------------------------------------------------------
        # Check for subreports
        # -------------------------------------------------------------
        $subreportNodes = $rdlXml.SelectNodes("//Subreport")
        if ($subreportNodes) {
            $subreportCount = $subreportNodes.Count
            $hasSubreports  = $subreportCount -gt 0
        }
    }
    catch {
        Write-Warning "Error analyzing $($report.Path): $_"
    }

    # Output the result with explicit types
    [PSCustomObject]@{
        ReportServerName        = $reportServerName
        ReportName              = $report.Name
        ReportPath              = $report.Path
        HasEmbeddedDataSources  = [bool]$hasEmbeddedDataSources
        EmbeddedDataSourceCount = [int]$embeddedDSCount
        HasEmbeddedCredentials  = [bool]$hasEmbeddedCreds
        EmbeddedCredTypes       = if ($credentialTypes.Count -gt 0) {
                                      ($credentialTypes -join ", ")
                                  } else { "None" }
        HasCustomCode           = [bool]$hasCustomCode
        HasExternalImages       = [bool]$hasExternalImages
        ExternalImageCount      = [int]$externalImageCount
        HasParameters           = [bool]$hasParameters
        ParameterCount          = [int]$parameterCount
        HasPotentialInjection   = [bool]$hasUnsafeExpressions
        HasSubreports           = [bool]$hasSubreports
        SubreportCount          = [int]$subreportCount
        ModifiedBy              = $report.ModifiedBy
        ModifiedDate            = $report.ModifiedDate
    }
}

# ---------------------------------------------------------------------
# Save report-audit CSV
# ---------------------------------------------------------------------
$reportAudit | Export-Csv -Path "$AuditOutputPath\PBIReportSecurityAudit.csv" -NoTypeInformation

# Calculate statistics
$totalReports           = $reportAudit.Count
$withEmbeddedDS         = ($reportAudit | Where-Object { $_.HasEmbeddedDataSources  -eq $true }).Count
$withSharedDS           = $totalReports - $withEmbeddedDS
$withEmbeddedCreds      = ($reportAudit | Where-Object { $_.HasEmbeddedCredentials -eq $true }).Count
$withCustomCode         = ($reportAudit | Where-Object { $_.HasCustomCode          -eq $true }).Count
$withExternalImages     = ($reportAudit | Where-Object { $_.HasExternalImages      -eq $true }).Count
$withParameters         = ($reportAudit | Where-Object { $_.HasParameters          -eq $true }).Count
$withPotentialInjection = ($reportAudit | Where-Object { $_.HasPotentialInjection  -eq $true }).Count
$withSubreports         = ($reportAudit | Where-Object { $_.HasSubreports          -eq $true }).Count
$withErrors             = ($reportAudit | Where-Object { $_.EmbeddedCredTypes -eq "XML_PARSE_ERROR" }).Count

# Build text summary for REPORTS (kept in variable, not written yet)
$reportSecReport = @"
==============================
POWER BI REPORT SECURITY AUDIT
Report Server: $reportServerName
Generated: $(Get-Date)

TOTAL REPORTS ANALYZED: $totalReports
Reports with XML Parse Errors: $withErrors

DATA SOURCE FINDINGS:
---------------------
Reports with SHARED Data Sources (GOOD): $withSharedDS
Reports with EMBEDDED Data Sources (RISK): $withEmbeddedDS

SECURITY FINDINGS:
------------------
Reports with Parameters: $withParameters
Reports with Embedded Credentials: $withEmbeddedCreds
Reports with Custom VSTA Code: $withCustomCode
Reports with External Images: $withExternalImages
Reports with Potential SQL Injection: $withPotentialInjection
Reports with Subreports: $withSubreports

RISK EXPLANATIONS:
------------------
1. EMBEDDED DATA SOURCES ($withEmbeddedDS reports)
   RISK: Bypass centralized credential management
   - Hard to audit and rotate credentials
   - Inconsistent security practices
   - Each report manages its own connection
   ACTION: Convert to shared data sources

2. EMBEDDED CREDENTIALS ($withEmbeddedCreds reports)
   RISK: Passwords stored in report definitions
   - Can be extracted by anyone with edit access
   - Not centrally managed or rotated
   - Visible in report XML
   ACTION: Remove immediately, use shared data sources

3. CUSTOM CODE ($withCustomCode reports)
   RISK: VB.NET code execution
   - Could access file system or network
   - Difficult to audit what code does
   - May have security vulnerabilities
   ACTION: Review all custom code, document purpose

4. EXTERNAL IMAGES ($withExternalImages reports)
   RISK: Content loaded from external URLs
   - Could be compromised or changed
   - Tracking/privacy concerns
   - Availability dependent on external site
   ACTION: Review URLs, consider embedding images

5. SQL INJECTION ($withPotentialInjection reports)
   RISK: Parameter concatenation in expressions
   - Parameters directly concatenated into SQL
   - Could allow SQL injection attacks
   - Bypasses proper parameterization
   ACTION: Rewrite queries to use proper parameters

RECOMMENDATIONS:
----------------
1. IMMEDIATE: Remove any embedded credentials from reports
2. HIGH: Convert embedded data sources to shared data sources
3. MEDIUM: Review and document all custom code usage
4. MEDIUM: Audit parameter usage for SQL injection risks
5. LOW: Review external image references

==============================
"@

# Console summary for report audit
Write-Host ""
Write-Host "==== REPORT AUDIT COMPLETE ====" -ForegroundColor Cyan
Write-Host "Report Server: $reportServerName" -ForegroundColor Cyan
Write-Host ""
Write-Host "Total Reports Analyzed: $totalReports" -ForegroundColor White
if ($withErrors -gt 0) {
    Write-Host "XML Parse Errors: $withErrors" -ForegroundColor Yellow
}
Write-Host ""
Write-Host "Data Source Analysis:" -ForegroundColor Cyan
Write-Host "  Shared Data Sources (GOOD): $withSharedDS" -ForegroundColor Green
Write-Host "  Embedded Data Sources (RISK): $withEmbeddedDS" -ForegroundColor $(if ($withEmbeddedDS -gt 0) { "Red" } else { "Green" })
Write-Host ""
Write-Host "Security Findings:" -ForegroundColor Yellow
Write-Host "  Parameters: $withParameters" -ForegroundColor White
Write-Host "  Embedded Credentials: $withEmbeddedCreds" -ForegroundColor $(if ($withEmbeddedCreds -gt 0) { "Red" } else { "Green" })
Write-Host "  Custom Code: $withCustomCode" -ForegroundColor $(if ($withCustomCode -gt 0) { "Yellow" } else { "Green" })
Write-Host "  External Images: $withExternalImages" -ForegroundColor $(if ($withExternalImages -gt 0) { "Yellow" } else { "Green" })
Write-Host "  Potential SQL Injection: $withPotentialInjection" -ForegroundColor $(if ($withPotentialInjection -gt 0) { "Red" } else { "Green" })
Write-Host "  Subreports: $withSubreports" -ForegroundColor White
Write-Host ""

Write-Host "=== HIGH RISK REPORTS ===" -ForegroundColor Red
$highRiskReports = $reportAudit | Where-Object {
    $_.HasEmbeddedCredentials -eq $true -or
    $_.HasCustomCode          -eq $true -or
    $_.HasPotentialInjection  -eq $true
}
if ($highRiskReports) {
    $highRiskReports | Format-Table ReportName, HasEmbeddedCredentials, HasCustomCode, HasPotentialInjection -AutoSize
    Write-Host ""
    Write-Host "CRITICAL: $($highRiskReports.Count) high-risk reports found!" -ForegroundColor Red
}
else {
    Write-Host "No high-risk reports found." -ForegroundColor Green
}

Write-Host ""
Write-Host "Report audit CSV: $AuditOutputPath\PBIReportSecurityAudit.csv" -ForegroundColor White
Write-Host ""


# =====================================================================
#  SECTION 2 – DATA SOURCE SECURITY AUDIT (CONNECTION STRINGS)
# =====================================================================

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "POWER BI DATA SOURCE SECURITY AUDIT (DSNs)"  -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Scanning for data sources..." -ForegroundColor Cyan
$allDataSources = Get-DataSourcesRecursive -Path "/" -Proxy $rs
Write-Host "Found $($allDataSources.Count) data sources on $reportServerName. Retrieving connection strings..." -ForegroundColor Cyan
Write-Host ""

$auditResults = foreach ($ds in $allDataSources) {
    Write-Host "Processing: $($ds.Path)" -ForegroundColor Gray

    try {
        $dsContent = $rs.GetDataSourceContents($ds.Path)
        $cs        = $dsContent.ConnectString

        # ---------------------------------------------------------
        # Extract server
        # ---------------------------------------------------------
        $server = "Unknown"
        if ($cs -match "server\s*=\s*([^;]+)") {
            $server = $matches[1].Trim()
        }
        elseif ($cs -match "data\s*source\s*=\s*([^;]+)") {
            $server = $matches[1].Trim()
        }

        # ---------------------------------------------------------
        # Extract username
        # ---------------------------------------------------------
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

        # ---------------------------------------------------------
        # Check for password in connection string
        # ---------------------------------------------------------
        $hasPasswordInCS = $cs -match "password\s*=" -or
                           $cs -match "pwd\s*="

        # ---------------------------------------------------------
        # Check for risky usernames
        # ---------------------------------------------------------
        $riskyUsernames  = @('sa', 'admin', 'administrator', 'root', 'dbo', 'sysadmin')
        $isRiskyUsername = $riskyUsernames -contains $username.ToLower()

        # ---------------------------------------------------------
        # Check authentication type
        # ---------------------------------------------------------
        $authType = "None"

        if ($cs -match "integrated\s*security\s*=\s*true" -or
            $cs -match "trusted_connection\s*=\s*yes" -or
            $dsContent.CredentialRetrieval -eq "Integrated") {

            $authType = "Windows"
        }
        elseif ($dsContent.CredentialRetrieval -eq "Store"  -or
                $cs -match "user\s*id\s*="                 -or
                $cs -match "uid\s*=") {

            $authType = "SQL"
        }
        elseif ($dsContent.CredentialRetrieval -eq "Prompt") {
            $authType = "Prompt"
        }

        [PSCustomObject]@{
            ReportServerName  = $reportServerName
            DataSourceName    = $ds.Name
            DataSourcePath    = $ds.Path
            Server            = $server
            Username          = $username
            ConnectionString  = $cs
            Provider          = $dsContent.Extension
            CredentialType    = $dsContent.CredentialRetrieval
            AuthType          = $authType
            HasPasswordInCS   = $hasPasswordInCS
            IsRiskyUsername   = $isRiskyUsername
            UsesStoredCreds   = ($dsContent.CredentialRetrieval -eq "Store")
            Enabled           = $dsContent.Enabled
            ModifiedBy        = $ds.ModifiedBy
            ModifiedDate      = $ds.ModifiedDate
        }
    }
    catch {
        Write-Warning "Could not retrieve details for $($ds.Path): $_"
    }
}

# ---------------------------------------------------------------------
# Export full audit for data sources
# ---------------------------------------------------------------------
$auditResults | Export-Csv -Path "$AuditOutputPath\PBIConnectionStrings.csv" -NoTypeInformation

# Generate compliance summary
$totalCount        = $auditResults.Count
$credTypeBreakdown = $auditResults |
                      Group-Object CredentialType |
                      ForEach-Object { " $($_.Name): $($_.Count)" }
$authTypeBreakdown = $auditResults |
                      Group-Object AuthType |
                      ForEach-Object { " $($_.Name): $($_.Count)" }

$passwordInCS  = ($auditResults | Where-Object { $_.HasPasswordInCS }).Count
$riskyUsers    = ($auditResults | Where-Object { $_.IsRiskyUsername }).Count
$storedCreds   = ($auditResults | Where-Object { $_.UsesStoredCreds }).Count
$sqlAuth       = ($auditResults | Where-Object { $_.AuthType -eq 'SQL' }).Count
$windowsAuth   = ($auditResults | Where-Object { $_.AuthType -eq 'Windows' }).Count

# Detailed DSN report (also kept in variable)
$report = @"
==============================
POWER BI DATA SOURCE SECURITY AUDIT
Report Server: $reportServerName
Generated: $(Get-Date)

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
----------------
1. Use Windows Authentication (Integrated Security) wherever possible
2. Avoid storing passwords in connection strings
3. Don't use 'sa' or other admin accounts for reporting
4. Use stored credentials only when necessary and rotate regularly
5. Consolidate to service accounts per server to reduce credential sprawl

==============================
"@

# ---------------------------------------------------------------------
# Combined TXT report (single narrative file)
# ---------------------------------------------------------------------
$combinedTextPath = "$AuditOutputPath\PBICombinedSecurityReport.txt"
($reportSecReport + "`r`n`r`n" + $report) | Out-File $combinedTextPath

# ---------------------------------------------------------------------
# Show high-risk data sources
# ---------------------------------------------------------------------
Write-Host "=== HIGH RISK DATA SOURCES ===" -ForegroundColor Red
$highRisk = $auditResults | Where-Object {
    $_.HasPasswordInCS -or
    $_.IsRiskyUsername -or
    ($_.UsesStoredCreds -and $_.AuthType -eq "SQL")
}

if ($highRisk) {
    $highRisk |
        Format-Table DataSourceName, Server, Username, AuthType, HasPasswordInCS, IsRiskyUsername -AutoSize
}
else {
    Write-Host "No high-risk data sources found." -ForegroundColor Green
}

# ---------------------------------------------------------------------
# Summary for DSN audit
# ---------------------------------------------------------------------
Write-Host ""
Write-Host "==== DATA SOURCE SUMMARY ====" -ForegroundColor Cyan
Write-Host "Report Server: $reportServerName" -ForegroundColor Cyan
Write-Host "Total Data Sources: $totalCount" -ForegroundColor White
Write-Host "High-Risk Data Sources: $($highRisk.Count)" -ForegroundColor $(if ($highRisk.Count -gt 0) { "Red" } else { "Green" })
Write-Host "SQL Authentication: $sqlAuth" -ForegroundColor $(if ($sqlAuth -gt 0) { "Yellow" } else { "Green" })
Write-Host "Windows Authentication: $windowsAuth" -ForegroundColor Green
Write-Host ""
Write-Host "Data source CSV:      $AuditOutputPath\PBIConnectionStrings.csv"       -ForegroundColor White
Write-Host "Combined TXT report:  $combinedTextPath"                               -ForegroundColor White
Write-Host ""

Write-Host "==== ALL AUDITS COMPLETE ====" -ForegroundColor Cyan
