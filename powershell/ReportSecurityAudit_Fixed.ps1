# ========================================
# POWER BI REPORT SERVER - REPORT SECURITY AUDIT
# Enhanced XML Parsing Version
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

function Parse-ReportDefinition {
    param(
        [byte[]]$ReportDefBytes,
        [string]$ReportPath
    )
    
    try {
        # Try UTF8 first
        $rdlText = [System.Text.Encoding]::UTF8.GetString($ReportDefBytes)
        
        # Clean the text
        $rdlText = $rdlText.Trim()
        
        # Remove BOM if present
        if ($rdlText[0] -eq [char]0xFEFF) {
            $rdlText = $rdlText.Substring(1)
        }
        
        # Try to parse as XML
        $rdlXml = New-Object System.Xml.XmlDocument
        $rdlXml.LoadXml($rdlText)
        
        return $rdlXml
    }
    catch {
        # If UTF8 fails, try other encodings
        try {
            $rdlText = [System.Text.Encoding]::Default.GetString($ReportDefBytes)
            $rdlText = $rdlText.Trim()
            
            $rdlXml = New-Object System.Xml.XmlDocument
            $rdlXml.LoadXml($rdlText)
            
            return $rdlXml
        }
        catch {
            Write-Warning "Cannot parse XML for $ReportPath : $_"
            return $null
        }
    }
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "POWER BI REPORT SECURITY AUDIT" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Scanning for reports..." -ForegroundColor Cyan
$allReports = Get-ReportsRecursive -Path "/" -Proxy $rs
Write-Host "Found $($allReports.Count) reports to analyze" -ForegroundColor Green
Write-Host ""

$successCount = 0
$errorCount = 0

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
    $parseError = $false
    
    try {
        # Get report definition
        $reportDef = $rs.GetItemDefinition($report.Path)
        
        # Parse the XML using our robust function
        $rdlXml = Parse-ReportDefinition -ReportDefBytes $reportDef -ReportPath $report.Path
        
        if ($rdlXml -eq $null) {
            $parseError = $true
            $errorCount++
        }
        else {
            $successCount++
            
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
                    }
                }
            }
            
            # Check for custom code
            if ($rdlXml.Report.Code) {
                $codeContent = $rdlXml.Report.Code.ToString().Trim()
                if ($codeContent -ne "") {
                    $hasCustomCode = $true
                }
            }
            
            # Check for external images
            try {
                $externalImageNodes = $rdlXml.SelectNodes("//Image[@Source='External']")
                if ($externalImageNodes) {
                    $externalImageCount = $externalImageNodes.Count
                    $hasExternalImages = $externalImageCount -gt 0
                }
            }
            catch {
                # Ignore XPath errors
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
            try {
                $subreportNodes = $rdlXml.SelectNodes("//Subreport")
                if ($subreportNodes) {
                    $subreportCount = $subreportNodes.Count
                    $hasSubreports = $subreportCount -gt 0
                }
            }
            catch {
                # Ignore XPath errors
            }
        }
    }
    catch {
        Write-Warning "Error analyzing $($report.Path): $_"
        $parseError = $true
        $errorCount++
    }
    
    # Output the result
    [PSCustomObject]@{
        ReportName = $report.Name
        ReportPath = $report.Path
        ParsedSuccessfully = [bool](-not $parseError)
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
$reportAudit | Export-Csv -Path "C:\Audits\ReportSecurityAudit_Final.csv" -NoTypeInformation

# Calculate statistics
$totalReports = $reportAudit.Count
$successfullyParsed = ($reportAudit | Where-Object {$_.ParsedSuccessfully -eq $true}).Count
$failedToParse = $totalReports - $successfullyParsed
$withEmbeddedDS = ($reportAudit | Where-Object {$_.HasEmbeddedDataSources -eq $true}).Count
$withSharedDS = $successfullyParsed - $withEmbeddedDS
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

TOTAL REPORTS: $totalReports
Successfully Parsed: $successfullyParsed
Parse Errors: $failedToParse

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

NOTE ON PARSE ERRORS:
---------------------
$failedToParse reports could not be parsed. Common causes:
- Corrupted RDL files
- Non-standard report formats
- Power BI Desktop reports (.pbix) - not RDL format
- Reports with encoding issues

These reports are included in the CSV with ParsedSuccessfully=False
and should be manually reviewed if they are critical.

RISK EXPLANATIONS:
------------------

1. EMBEDDED DATA SOURCES ($withEmbeddedDS reports)
   RISK: Bypass centralized credential management
   ACTION: Convert to shared data sources
   
2. EMBEDDED CREDENTIALS ($withEmbeddedCreds reports)
   RISK: Passwords stored in report definitions
   ACTION: Remove immediately, use shared data sources
   
3. CUSTOM CODE ($withCustomCode reports)
   RISK: VB.NET code execution
   ACTION: Review all custom code, document purpose
   
4. EXTERNAL IMAGES ($withExternalImages reports)
   RISK: Content loaded from external URLs
   ACTION: Review URLs, consider embedding images
   
5. SQL INJECTION ($withPotentialInjection reports)
   RISK: Parameter concatenation in expressions
   ACTION: Rewrite queries to use proper parameters

RECOMMENDATIONS:
----------------
1. IMMEDIATE: Remove any embedded credentials from reports
2. HIGH: Convert embedded data sources to shared data sources
3. MEDIUM: Review and document all custom code usage
4. MEDIUM: Audit parameter usage for SQL injection risks
5. LOW: Review external image references
6. ONGOING: Manually review reports that failed to parse

========================================
"@

$reportSecReport | Out-File "C:\Audits\ReportSecurityReport_Final.txt"

# Display summary
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "AUDIT COMPLETE" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Total Reports: $totalReports" -ForegroundColor White
Write-Host "Successfully Parsed: $successfullyParsed" -ForegroundColor Green
if ($failedToParse -gt 0) {
    Write-Host "Parse Errors: $failedToParse" -ForegroundColor Yellow
    Write-Host "(These reports should be manually reviewed)" -ForegroundColor Gray
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
    $_.ParsedSuccessfully -eq $true -and (
        $_.HasEmbeddedCredentials -eq $true -or 
        $_.HasCustomCode -eq $true -or 
        $_.HasPotentialInjection -eq $true
    )
}

if ($highRiskReports) {
    $highRiskReports | Format-Table ReportName, HasEmbeddedCredentials, HasCustomCode, HasPotentialInjection -AutoSize
    Write-Host ""
    Write-Host "CRITICAL: $($highRiskReports.Count) high-risk reports found!" -ForegroundColor Red
}
else {
    Write-Host "No high-risk reports found!" -ForegroundColor Green
}

# Show reports that failed to parse
if ($failedToParse -gt 0) {
    Write-Host ""
    Write-Host "=== REPORTS WITH PARSE ERRORS ===" -ForegroundColor Yellow
    $failedReports = $reportAudit | Where-Object {$_.ParsedSuccessfully -eq $false}
    $failedReports | Format-Table ReportName, ReportPath -AutoSize
    Write-Host "These $failedToParse reports should be manually reviewed." -ForegroundColor Gray
}

Write-Host ""
Write-Host "Output Files:" -ForegroundColor Cyan
Write-Host "  Full audit: C:\Audits\ReportSecurityAudit_Final.csv" -ForegroundColor White
Write-Host "  Summary report: C:\Audits\ReportSecurityReport_Final.txt" -ForegroundColor White
Write-Host ""