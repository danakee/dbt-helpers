# ========================================
# POWER BI REPORT SERVER - REPORT SECURITY AUDIT
# Fixed XML Parsing Version
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
    
    # Initialize all variables with defaults
    $hasEmbeddedDataSources = $false
    $embeddedDSCount = 0
    $hasEmbeddedCreds = $false
    $credentialTypes = @()
    $hasCustomCode = $false
    $hasExternalImages = $false
    $externalImageCount = 0
    $hasParameters = $false
    $parameterCount = 0
    $hasUnsafeExpressions = $false
    $hasSubreports = $false
    $subreportCount = 0
    
    try {
        # Get report definition
        $reportDef = $rs.GetItemDefinition($report.Path)
        $rdlText = [System.Text.Encoding]::UTF8.GetString($reportDef)
        
        # Clean the XML - remove BOM and problematic characters
        $rdlText = $rdlText.Trim()
        # Remove BOM if present
        if ($rdlText.StartsWith([char]0xFEFF)) {
            $rdlText = $rdlText.Substring(1)
        }
        
        # Parse XML with better error handling
        try {
            [xml]$rdlXml = $rdlText
        }
        catch {
            Write-Warning "XML parse error for $($report.Path): $_"
            # Skip this report if XML won't parse
            [PSCustomObject]@{
                ReportName = $report.Name
                ReportPath = $report.Path
                HasEmbeddedDataSources = $false
                EmbeddedDataSourceCount = 0
                HasEmbeddedCredentials = $false
                EmbeddedCredTypes = "XML_PARSE_ERROR"
                HasCustomCode = $false
                HasExternalImages = $false
                ExternalImageCount = 0
                HasParameters = $false
                ParameterCount = 0
                HasPotentialInjection = $false
                HasSubreports = $false
                SubreportCount = 0
                ModifiedBy = $report.ModifiedBy
                ModifiedDate = $report.ModifiedDate
            }
            continue
        }
        
        # Check for data sources
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
                    # Shared = has DataSourceReference
                    if ($ds.ConnectionProperties) {
                        $hasEmbeddedDataSources = $true
                        $embeddedDSCount++
                        
                        $connString = $ds.ConnectionProperties.ConnectString
                        if ($connString) {
                            # Check for password in connection string
                            if ($connString -match "password\s*=|pwd\s*=") {
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
        
        # Check for custom code
        if ($rdlXml.Report.Code) {
            $codeContent = $rdlXml.Report.Code.Trim()
            if ($codeContent -ne "") {
                $hasCustomCode = $true
            }
        }
        
        # Check for external images
        $externalImageNodes = $rdlXml.SelectNodes("//Image[@Source='External']")
        if ($externalImageNodes) {
            $externalImageCount = $externalImageNodes.Count
            $hasExternalImages = $externalImageCount -gt 0
        }
        
        # Check for parameters
        if ($rdlXml.Report.ReportParameters.ReportParameter) {
            $params = $rdlXml.Report.ReportParameters.ReportParameter
            if ($params -isnot [System.Array]) {
                $params = @($params)
            }
            $parameterCount = $params.Count
            $hasParameters = $parameterCount -gt 0
        }
        
        # Check for unsafe parameter expressions (SQL injection risk)
        $rdlContent = $rdlXml.InnerXml
        if ($rdlContent -match '=.*Parameters!.*\+\s*"' -or 
            $rdlContent -match '=.*Parameters!.*&amp;\s*"') {
            $hasUnsafeExpressions = $true
        }
        
        # Check for subreports
        $subreportNodes = $rdlXml.SelectNodes("//Subreport")
        if ($subreportNodes) {
            $subreportCount = $subreportNodes.Count
            $hasSubreports = $subreportCount -gt 0
        }
        
    }
    catch {
        Write-Warning "Error analyzing $($report.Path): $_"
    }
    
    # Output the result with explicit types
    [PSCustomObject]@{
        ReportName = $report.Name
        ReportPath = $report.Path
        HasEmbeddedDataSources = [bool]$hasEmbeddedDataSources
        EmbeddedDataSourceCount = [int]$embeddedDSCount
        HasEmbeddedCredentials = [bool]$hasEmbeddedCreds
        EmbeddedCredTypes = if ($credentialTypes.Count -gt 0) {($credentialTypes -join ", ")} else {"None"}
        HasCustomCode = [bool]$hasCustomCode
        HasExternalImages = [bool]$hasExternalImages
        ExternalImageCount = [int]$externalImageCount
        HasParameters = [bool]$hasParameters
        ParameterCount = [int]$parameterCount
        HasPotentialInjection = [bool]$hasUnsafeExpressions
        HasSubreports = [bool]$hasSubreports
        SubreportCount = [int]$subreportCount
        ModifiedBy = $report.ModifiedBy
        ModifiedDate = $report.ModifiedDate
    }
}

# Create output directory
New-Item -Path "C:\Audits" -ItemType Directory -Force | Out-Null

# Export results
$reportAudit | Export-Csv -Path "C:\Audits\ReportSecurityAudit_Fixed.csv" -NoTypeInformation

# Calculate statistics
$totalReports = $reportAudit.Count
$withEmbeddedDS = ($reportAudit | Where-Object {$_.HasEmbeddedDataSources -eq $true}).Count
$withSharedDS = $totalReports - $withEmbeddedDS
$withEmbeddedCreds = ($reportAudit | Where-Object {$_.HasEmbeddedCredentials -eq $true}).Count
$withCustomCode = ($reportAudit | Where-Object {$_.HasCustomCode -eq $true}).Count
$withExternalImages = ($reportAudit | Where-Object {$_.HasExternalImages -eq $true}).Count
$withPotentialInjection = ($reportAudit | Where-Object {$_.HasPotentialInjection -eq $true}).Count
$withSubreports = ($reportAudit | Where-Object {$_.HasSubreports -eq $true}).Count
$withErrors = ($reportAudit | Where-Object {$_.EmbeddedCredTypes -eq "XML_PARSE_ERROR"}).Count

# Generate detailed report
$reportSecReport = @"
========================================
POWER BI REPORT SECURITY AUDIT
Generated: $(Get-Date)
========================================

TOTAL REPORTS ANALYZED: $totalReports
Reports with XML Parse Errors: $withErrors

DATA SOURCE FINDINGS:
---------------------
Reports with SHARED Data Sources (GOOD): $withSharedDS
Reports with EMBEDDED Data Sources (RISK): $withEmbeddedDS

SECURITY FINDINGS:
------------------
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

RECOMMENDATIONS:
----------------
1. IMMEDIATE: Remove any embedded credentials from reports
2. HIGH: Convert embedded data sources to shared data sources
3. MEDIUM: Review and document all custom code usage
4. MEDIUM: Audit parameter usage for SQL injection risks
5. LOW: Review external image references

========================================
"@

$reportSecReport | Out-File "C:\Audits\ReportSecurityReport_Fixed.txt"

# Display summary
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "AUDIT COMPLETE" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Total Reports Analyzed: $totalReports" -ForegroundColor White
if ($withErrors -gt 0) {
    Write-Host "XML Parse Errors: $withErrors" -ForegroundColor Yellow
}
Write-Host ""
Write-Host "Data Source Analysis:" -ForegroundColor Cyan
Write-Host "  Shared Data Sources (Good): $withSharedDS" -ForegroundColor Green
Write-Host "  Embedded Data Sources (Risk): $withEmbeddedDS" -ForegroundColor $(if ($withEmbeddedDS -gt 0) {"Red"} else {"Green"})
Write-Host ""
Write-Host "Security Findings:" -ForegroundColor Yellow
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
Write-Host "  Full audit: C:\Audits\ReportSecurityAudit_Fixed.csv" -ForegroundColor White
Write-Host "  Summary report: C:\Audits\ReportSecurityReport_Fixed.txt" -ForegroundColor White
Write-Host ""