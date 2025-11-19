# ========================================
# DIAGNOSE FAILED REPORTS - FIXED
# ========================================

$reportServerUri = "http://your-pbirs-server/ReportServer/ReportService2010.asmx?wsdl"
$rs = New-WebServiceProxy -Uri $reportServerUri -UseDefaultCredential

Write-Host "Checking for audit file..." -ForegroundColor Cyan

# Try to find the most recent audit file
$auditFiles = @(
    "C:\Audits\ReportSecurityAudit_Final.csv",
    "C:\Audits\ReportSecurityAudit_Fixed.csv",
    "C:\Audits\ReportSecurityAudit.csv"
)

$auditFile = $null
foreach ($file in $auditFiles) {
    if (Test-Path $file) {
        $auditFile = $file
        Write-Host "Found audit file: $file" -ForegroundColor Green
        break
    }
}

if (-not $auditFile) {
    Write-Host "ERROR: No audit file found!" -ForegroundColor Red
    exit
}

# Import the audit
$audit = Import-Csv $auditFile

# Find failed reports
$failedReports = $audit | Where-Object {
    $_.EmbeddedCredTypes -eq "XML_PARSE_ERROR"
}

if ($failedReports.Count -eq 0) {
    Write-Host "No failed reports found in the audit!" -ForegroundColor Green
    exit
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "DIAGNOSING $($failedReports.Count) FAILED REPORTS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$diagnostics = foreach ($report in $failedReports) {
    Write-Host "Analyzing: $($report.ReportPath)" -ForegroundColor Yellow
    
    try {
        # Get report definition directly - don't call ListChildren
        $reportDef = $rs.GetItemDefinition($report.ReportPath)
        $rdlText = [System.Text.Encoding]::UTF8.GetString($reportDef)
        
        # Clean the text
        $rdlText = $rdlText.Trim()
        if ($rdlText.Length -gt 0 -and $rdlText[0] -eq [char]0xFEFF) {
            $rdlText = $rdlText.Substring(1)
        }
        
        # Extract key information
        $fileSize = $reportDef.Length
        $firstLine = ""
        if ($rdlText.Length -gt 0) {
            $lines = $rdlText -split "`n"
            if ($lines.Count -gt 0) {
                $firstLine = $lines[0].Trim()
            }
        }
        
        # Try to identify RDL schema version
        $schemaVersion = "Unknown"
        if ($rdlText -match 'xmlns="http://schemas\.microsoft\.com/sqlserver/reporting/(\d+)/(\d+)/reportdefinition"') {
            $schemaVersion = "$($matches[1])/$($matches[2])"
        }
        elseif ($rdlText -match 'reportdefinition/(\d+)/(\d+)') {
            $schemaVersion = "$($matches[1])/$($matches[2])"
        }
        
        # Check report type indicators
        $isPowerBI = $rdlText -match "powerbi" -or $report.ReportPath -match "\.pbix"
        $isPagedReport = $rdlText -match '<Report xmlns=' -and $rdlText -match 'reportdefinition'
        
        # Check for namespace
        $hasNamespace = $rdlText -match 'xmlns='
        
        # Try to find the Report node
        $hasReportNode = $rdlText -match '<Report'
        
        # Check encoding
        $encodingDecl = "None"
        if ($rdlText -match '\<\?xml.*encoding="([^"]+)"') {
            $encodingDecl = $matches[1]
        }
        
        # Count elements
        $dataSourceCount = ([regex]::Matches($rdlText, '<DataSource')).Count
        $dataSetCount = ([regex]::Matches($rdlText, '<DataSet')).Count
        
        # Check for BOM
        $hasBOM = $reportDef.Length -ge 3 -and $reportDef[0] -eq 0xEF -and $reportDef[1] -eq 0xBB -and $reportDef[2] -eq 0xBF
        
        # Determine report format
        $reportFormat = "Unknown"
        if ($isPagedReport) {
            $reportFormat = "Paginated Report (RDL)"
        }
        elseif ($isPowerBI) {
            $reportFormat = "Power BI Report"
        }
        elseif ($rdlText -match '<Report') {
            $reportFormat = "Legacy Report"
        }
        
        # Try to parse and identify specific error
        $errorType = "Unknown"
        $errorDetail = ""
        try {
            $xmlDoc = New-Object System.Xml.XmlDocument
            $xmlDoc.LoadXml($rdlText)
            $errorType = "No Error (Parsed Successfully)"
        }
        catch {
            $errorMsg = $_.Exception.Message
            $errorDetail = $errorMsg
            
            if ($errorMsg -match "specified node cannot be inserted") {
                $errorType = "Invalid child node structure"
            }
            elseif ($errorMsg -match "unexpected end") {
                $errorType = "Truncated/incomplete XML"
            }
            elseif ($errorMsg -match "Data at the root level") {
                $errorType = "Invalid root element"
            }
            elseif ($errorMsg -match "namespace") {
                $errorType = "Namespace conflict"
            }
            elseif ($errorMsg -match "hexadecimal") {
                $errorType = "Invalid character encoding"
            }
            elseif ($errorMsg -match "not expected") {
                $errorType = "Unexpected XML element"
            }
            else {
                $errorType = "XML Parse Error"
            }
        }
        
        # Extract a sample of the problematic area if possible
        $problemArea = ""
        if ($errorDetail -match "Line (\d+)") {
            $lineNum = [int]$matches[1]
            $lines = $rdlText -split "`n"
            if ($lineNum -le $lines.Count) {
                $startLine = [Math]::Max(0, $lineNum - 2)
                $endLine = [Math]::Min($lines.Count - 1, $lineNum + 1)
                $problemArea = ($lines[$startLine..$endLine] -join " | ").Substring(0, [Math]::Min(200, ($lines[$startLine..$endLine] -join " | ").Length))
            }
        }
        
        [PSCustomObject]@{
            ReportName = $report.ReportName
            ReportPath = $report.ReportPath
            ReportFormat = $reportFormat
            FileSizeKB = [Math]::Round($fileSize / 1024, 2)
            SchemaVersion = $schemaVersion
            ErrorType = $errorType
            ErrorDetail = $errorDetail.Substring(0, [Math]::Min(200, $errorDetail.Length))
            HasNamespace = $hasNamespace
            HasReportNode = $hasReportNode
            EncodingDecl = $encodingDecl
            HasBOM = $hasBOM
            DataSourceCount = $dataSourceCount
            DataSetCount = $dataSetCount
            ProblemArea = $problemArea
            FirstLine = if ($firstLine.Length -gt 0) { $firstLine.Substring(0, [Math]::Min(150, $firstLine.Length)) } else { "Empty" }
            ModifiedDate = $report.ModifiedDate
            ModifiedBy = $report.ModifiedBy
        }
    }
    catch {
        Write-Warning "Error diagnosing $($report.ReportPath): $_"
        
        [PSCustomObject]@{
            ReportName = $report.ReportName
            ReportPath = $report.ReportPath
            ReportFormat = "ERROR"
            FileSizeKB = 0
            SchemaVersion = "ERROR"
            ErrorType = "Cannot access report"
            ErrorDetail = $_.Exception.Message
            HasNamespace = $false
            HasReportNode = $false
            EncodingDecl = "ERROR"
            HasBOM = $false
            DataSourceCount = 0
            DataSetCount = 0
            ProblemArea = ""
            FirstLine = "ERROR"
            ModifiedDate = $report.ModifiedDate
            ModifiedBy = $report.ModifiedBy
        }
    }
}

# Export diagnostics
$diagnostics | Export-Csv "C:\Audits\FailedReports_Diagnostics.csv" -NoTypeInformation

# Display summary
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "DIAGNOSTIC SUMMARY" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Categorize by folder
Write-Host "Reports by Folder:" -ForegroundColor Yellow
$diagnostics | ForEach-Object {
    $folder = Split-Path $_.ReportPath -Parent
    [PSCustomObject]@{Folder = $folder; Report = $_.ReportName}
} | Group-Object Folder | Sort-Object Count -Descending | ForEach-Object {
    Write-Host "  $($_.Name): $($_.Count)" -ForegroundColor White
}
Write-Host ""

# Group by error type
Write-Host "Errors by Type:" -ForegroundColor Yellow
$diagnostics | Group-Object ErrorType | Sort-Object Count -Descending | ForEach-Object {
    Write-Host "  $($_.Name): $($_.Count)" -ForegroundColor White
}
Write-Host ""

# Group by schema version
Write-Host "Reports by Schema Version:" -ForegroundColor Yellow
$diagnostics | Group-Object SchemaVersion | Sort-Object Count -Descending | ForEach-Object {
    Write-Host "  $($_.Name): $($_.Count)" -ForegroundColor White
}
Write-Host ""

# Group by report format
Write-Host "Reports by Format:" -ForegroundColor Yellow
$diagnostics | Group-Object ReportFormat | Sort-Object Count -Descending | ForEach-Object {
    Write-Host "  $($_.Name): $($_.Count)" -ForegroundColor White
}
Write-Host ""

# Show detailed list
Write-Host "=== FAILED REPORTS DETAIL ===" -ForegroundColor Red
$diagnostics | Format-Table ReportName, SchemaVersion, ErrorType, FileSizeKB -AutoSize

Write-Host ""
Write-Host "Full diagnostics exported to: C:\Audits\FailedReports_Diagnostics.csv" -ForegroundColor Cyan
Write-Host ""

# Check if all are in deprecated folder
$deprecatedCount = ($diagnostics | Where-Object {$_.ReportPath -match "Deprecated"}).Count

if ($deprecatedCount -eq $diagnostics.Count) {
    Write-Host "========================================" -ForegroundColor Yellow
    Write-Host "IMPORTANT FINDING!" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Yellow
    Write-Host "ALL $deprecatedCount failed reports are in 'Deprecated' folders!" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Recommendation:" -ForegroundColor Cyan
    Write-Host "  - These reports appear to be intentionally deprecated" -ForegroundColor White
    Write-Host "  - Consider archiving or removing them if no longer needed" -ForegroundColor White
    Write-Host "  - If they're truly deprecated, XML parsing issues are less critical" -ForegroundColor White
    Write-Host ""
}

# Provide specific recommendations
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "RECOMMENDATIONS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$errorGroups = $diagnostics | Group-Object ErrorType

foreach ($group in $errorGroups | Sort-Object Count -Descending) {
    Write-Host "$($group.Name) - $($group.Count) reports:" -ForegroundColor Yellow
    
    switch -Wildcard ($group.Name) {
        "*Invalid child node*" {
            Write-Host "  Issue: XML structure has elements in wrong order" -ForegroundColor White
            Write-Host "  Action: Open in Report Builder, may auto-fix on save" -ForegroundColor Green
        }
        "*Truncated*" {
            Write-Host "  Issue: File is incomplete" -ForegroundColor White
            Write-Host "  Action: Restore from backup or recreate" -ForegroundColor Green
        }
        "*Namespace*" {
            Write-Host "  Issue: XML namespace problems" -ForegroundColor White
            Write-Host "  Action: May need manual XML editing" -ForegroundColor Green
        }
        "*character*" {
            Write-Host "  Issue: Invalid characters in XML" -ForegroundColor White
            Write-Host "  Action: Check file encoding" -ForegroundColor Green
        }
        "*No Error*" {
            Write-Host "  Note: These parsed successfully now - may have been transient" -ForegroundColor Green
        }
        default {
            Write-Host "  Check ErrorDetail column in CSV for specifics" -ForegroundColor Gray
        }
    }
    Write-Host ""
}

Write-Host "========================================" -ForegroundColor Green
Write-Host "NEXT STEPS" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "1. Review the CSV: C:\Audits\FailedReports_Diagnostics.csv" -ForegroundColor White
Write-Host "2. Check 'ErrorDetail' and 'ProblemArea' columns for specific issues" -ForegroundColor White
Write-Host "3. If reports are in 'Deprecated' folders, consider archiving" -ForegroundColor White
Write-Host "4. For active reports, try opening in Report Builder" -ForegroundColor White
Write-Host "5. Document any reports that cannot be fixed" -ForegroundColor White
Write-Host ""