# ========================================
# POWER BI REPORT SERVER - REPORT SECURITY AUDIT
# PowerShell 5.1 Compatible
# ========================================

$reportServerUri = "http://your-pbirs-server/ReportServer/ReportService2010.asmx?wsdl"
$rs = New-WebServiceProxy -Uri $reportServerUri -UseDefaultCredential

function Get-ReportsRecursive {
    param([string]$Path = "/", [object]$Proxy)
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

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "POWER BI REPORT SECURITY AUDIT" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Scanning for reports..." -ForegroundColor Cyan
$allReports = Get-ReportsRecursive -Path "/" -Proxy $rs
Write-Host "Found $($allReports.Count) reports to analyze" -ForegroundColor Green
Write-Host ""

$reportAudit = foreach ($report in $allReports) {
    Write-Host "Analyzing: $($report.Path)" -ForegroundColor Gray
    
    try {
        # Get report definition (RDL XML)
        $reportDef = $rs.GetItemDefinition($report.Path)
        $rdl = [System.Text.Encoding]::UTF8.GetString($reportDef)
        
        # Parse as XML
        [xml]$rdlXml = $rdl
        
        # Check for embedded data sources
        $embeddedDataSources = @()
        if ($rdlXml.Report.DataSources.DataSource) {
            $embeddedDataSources = $rdlXml.Report.DataSources.DataSource | Where-Object {
                $_.ConnectionProperties -ne $null
            }
        }
        
        # Check for credentials in embedded sources
        $hasEmbeddedCreds = $false
        $embeddedConnStrings = @()
        $credentialTypes = @()
        
        if ($embeddedDataSources) {
            foreach ($ds in $embeddedDataSources) {
                if ($ds.ConnectionProperties.ConnectString) {
                    $connString = $ds.ConnectionProperties.ConnectString
                    $embeddedConnStrings += $connString
                    
                    # Check for embedded credentials
                    if ($connString -match "password\s*=|pwd\s*=") {
                        $hasEmbeddedCreds = $true
                    }
                    
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
        }
        
        # Check for custom code
        $hasCustomCode = $false
        if ($rdlXml.Report.Code -ne $null) {
            $codeContent = $rdlXml.Report.Code
            if ($codeContent -and $codeContent.Trim() -ne "") {
                $hasCustomCode = $true
            }
        }
        
        # Check for external images
        $externalImages = $rdlXml.SelectNodes("//Image[@Source='External']")
        $hasExternalImages = $false
        $externalImageCount = 0
        if ($externalImages) {
            $externalImageCount = $externalImages.Count
            $hasExternalImages = $externalImageCount -gt 0
        }
        
        # Check for report parameters
        $parameterCount = 0
        $hasParameters = $false
        if ($rdlXml.Report.ReportParameters.ReportParameter) {
            $parameters = $rdlXml.Report.ReportParameters.ReportParameter
            if ($parameters) {
                $parameterCount = @($parameters).Count
                $hasParameters = $parameterCount -gt 0
            }
        }
        
        # Check for expressions that might use parameters unsafely (SQL injection risk)
        $hasUnsafeExpressions = $false
        $rdlContent = $rdlXml.InnerXml
        if ($rdlContent -match '=.*Parameters!.*\+.*"' -or 
            $rdlContent -match '=.*Parameters!.*&amp;') {
            $hasUnsafeExpressions = $true
        }
        
        # Check for subreports (can be a security concern)
        $subreports = $rdlXml.SelectNodes("//Subreport")
        $hasSubreports = $false
        $subreportCount = 0
        if ($subreports) {
            $subreportCount = $subreports.Count
            $hasSubreports = $subreportCount -gt 0
        }
        
        [PSCustomObject]@{
            ReportName = $report.Name
            ReportPath = $report.Path
            HasEmbeddedDataSources = ($embeddedDataSources.Count -gt 0)
            EmbeddedDataSourceCount = $embeddedDataSources.Count
            HasEmbeddedCredentials = $hasEmbeddedCreds
            EmbeddedCredTypes = ($credentialTypes -join ", ")
            HasCustomCode = $hasCustomCode
            HasExternalImages = $hasExternalImages
            ExternalImageCount = $externalImageCount
            HasParameters = $hasParameters
            ParameterCount = $parameterCount
            HasPotentialInjection = $hasUnsafeExpressions
            HasSubreports = $hasSubreports
            SubreportCount = $subreportCount
            ModifiedBy = $report.ModifiedBy
            ModifiedDate = $report.ModifiedDate
        }
    }
    catch {
        Write-Warning "Error analyzing $($report.Path): $_"
        
        [PSCustomObject]@{
            ReportName = $report.Name
            ReportPath = $report.Path
            HasEmbeddedDataSources = $null
            EmbeddedDataSourceCount = 0
            HasEmbeddedCredentials = $null
            EmbeddedCredTypes = "ERROR"
            HasCustomCode = $null
            HasExternalImages = $null
            ExternalImageCount = 0
            HasParameters = $null
            ParameterCount = 0
            HasPotentialInjection = $null
            HasSubreports = $null
            SubreportCount = 0
            ModifiedBy = $report.ModifiedBy
            ModifiedDate = $report.ModifiedDate
        }
    }
}

# Create output directory
New-Item -Path "C:\Audits" -ItemType Directory -Force | Out-Null

# Export results
$reportAudit | Export-Csv -Path "C:\Audits\ReportSecurityAudit.csv" -NoTypeInformation

# Calculate statistics
$totalReports = $reportAudit.Count
$withEmbeddedDS = ($reportAudit | Where-Object {$_.HasEmbeddedDataSources -eq $true}).Count
$withEmbeddedCreds = ($reportAudit | Where-Object {$_.HasEmbeddedCredentials -eq $true}).Count
$withCustomCode = ($reportAudit | Where-Object {$_.HasCustomCode -eq $true}).Count
$withExternalImages = ($reportAudit | Where-Object {$_.HasExternalImages -eq $true}).Count
$withPotentialInjection = ($reportAudit | Where-Object {$_.HasPotentialInjection -eq $true}).Count
$withSubreports = ($reportAudit | Where-Object {$_.HasSubreports -eq $true}).Count

# Generate detailed report
$reportSecReport = @"
========================================
POWER BI REPORT SECURITY AUDIT
Generated: $(Get-Date)
========================================

TOTAL REPORTS ANALYZED: $totalReports

SECURITY FINDINGS:
------------------
Reports with Embedded Data Sources: $withEmbeddedDS
Reports with Embedded Credentials: $withEmbeddedCreds
Reports with Custom VB Code: $withCustomCode
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
   
6. SUBREPORTS ($withSubreports reports)
   INFO: Reports that call other reports
   - Complex permission chains
   - Performance considerations
   - May bypass security boundaries
   ACTION: Review subreport usage and permissions

RECOMMENDATIONS:
----------------
1. IMMEDIATE: Remove any embedded credentials from reports
2. HIGH: Convert embedded data sources to shared data sources
3. MEDIUM: Review and document all custom code usage
4. MEDIUM: Audit parameter usage for SQL injection risks
5. LOW: Review external image references
6. ONGOING: Use report execution logs to identify unused reports

========================================
"@

$reportSecReport | Out-File "C:\Audits\ReportSecurityReport.txt"

# Display summary
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "AUDIT COMPLETE" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Total Reports Analyzed: $totalReports" -ForegroundColor White
Write-Host ""
Write-Host "Security Findings:" -ForegroundColor Yellow
Write-Host "  Embedded Data Sources: $withEmbeddedDS" -ForegroundColor $(if ($withEmbeddedDS -gt 0) {"Red"} else {"Green"})
Write-Host "  Embedded Credentials: $withEmbeddedCreds" -ForegroundColor $(if ($withEmbeddedCreds -gt 0) {"Red"} else {"Green"})
Write-Host "  Custom Code: $withCustomCode" -ForegroundColor $(if ($withCustomCode -gt 0) {"Yellow"} else {"Green"})
Write-Host "  External Images: $withExternalImages" -ForegroundColor $(if ($withExternalImages -gt 0) {"Yellow"} else {"Green"})
Write-Host "  Potential SQL Injection: $withPotentialInjection" -ForegroundColor $(if ($withPotentialInjection -gt 0) {"Red"} else {"Green"})
Write-Host "  Subreports: $withSubreports" -ForegroundColor White
Write-Host ""

# Show high-risk reports
Write-Host "=== HIGH RISK REPORTS ===" -ForegroundColor Red
$highRiskReports = $reportAudit | Where-Object {
    $_.HasEmbeddedCredentials -eq $true -or 
    $_.HasCustomCode -eq $true -or 
    $_.HasPotentialInjection -eq $true
}

if ($highRiskReports) {
    $highRiskReports | Format-Table ReportName, HasEmbeddedCredentials, HasCustomCode, HasPotentialInjection -AutoSize
    Write-Host ""
    Write-Host "CRITICAL: $($highRiskReports.Count) high-risk reports found!" -ForegroundColor Red
}
else {
    Write-Host "No high-risk reports found!" -ForegroundColor Green
}

Write-Host ""
Write-Host "Output Files:" -ForegroundColor Cyan
Write-Host "  Full audit: C:\Audits\ReportSecurityAudit.csv" -ForegroundColor White
Write-Host "  Summary report: C:\Audits\ReportSecurityReport.txt" -ForegroundColor White
Write-Host ""