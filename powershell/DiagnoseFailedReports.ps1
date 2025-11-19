# ========================================
# DIAGNOSE FAILED REPORTS
# ========================================

$reportServerUri = "http://your-pbirs-server/ReportServer/ReportService2010.asmx?wsdl"
$rs = New-WebServiceProxy -Uri $reportServerUri -UseDefaultCredential

# Import the audit to find failed reports
$audit = Import-Csv "C:\Audits\ReportSecurityAudit_Final.csv"

$failedReports = $audit | Where-Object {$_.ParsedSuccessfully -eq "False"}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "DIAGNOSING $($failedReports.Count) FAILED REPORTS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$diagnostics = foreach ($report in $failedReports) {
    Write-Host "Analyzing: $($report.ReportPath)" -ForegroundColor Yellow
    
    try {
        # Get the report item details
        $itemDetails = $rs.ListChildren($report.ReportPath, $false) | Where-Object {$_.Path -eq $report.ReportPath}
        if (-not $itemDetails) {
            # Try getting parent folder and finding the item
            $parentPath = Split-Path $report.ReportPath -Parent
            if ($parentPath -eq "") { $parentPath = "/" }
            $itemDetails = $rs.ListChildren($parentPath, $false) | Where-Object {$_.Path -eq $report.ReportPath}
        }
        
        # Get report definition
        $reportDef = $rs.GetItemDefinition($report.ReportPath)
        $rdlText = [System.Text.Encoding]::UTF8.GetString($reportDef)
        
        # Clean the text
        $rdlText = $rdlText.Trim()
        if ($rdlText[0] -eq [char]0xFEFF) {
            $rdlText = $rdlText.Substring(1)
        }
        
        # Extract key information
        $fileSize = $reportDef.Length
        $firstLine = ($rdlText -split "`n")[0]
        
        # Try to identify RDL schema version
        $schemaVersion = "Unknown"
        if ($rdlText -match 'xmlns="http://schemas\.microsoft\.com/sqlserver/reporting/(\d+)/(\d+)/reportdefinition"') {
            $schemaVersion = "$($matches[1])/$($matches[2])"
        }
        elseif ($rdlText -match 'reportdefinition/(\d+)/(\d+)') {
            $schemaVersion = "$($matches[1])/$($matches[2])"
        }
        
        # Check if it's a Power BI report
        $isPowerBI = $rdlText -match "powerbi" -or $report.ReportPath -match "\.pbix"
        
        # Check for namespace issues
        $hasNamespace = $rdlText -match 'xmlns='
        
        # Try to find the Report node
        $hasReportNode = $rdlText -match '<Report'
        
        # Check encoding declaration
        $encodingDecl = "None"
        if ($rdlText -match '\<\?xml.*encoding="([^"]+)"') {
            $encodingDecl = $matches[1]
        }
        
        # Count major elements
        $dataSourceCount = ([regex]::Matches($rdlText, '<DataSource')).Count
        $dataSetCount = ([regex]::Matches($rdlText, '<DataSet')).Count
        
        # Check for special characters that might cause issues
        $hasBOM = $reportDef[0] -eq 0xEF -and $reportDef[1] -eq 0xBB -and $reportDef[2] -eq 0xBF
        
        # Try to identify the specific error
        $errorType = "Unknown"
        try {
            $xmlDoc = New-Object System.Xml.XmlDocument
            $xmlDoc.LoadXml($rdlText)
            $errorType = "No error detected (should have parsed)"
        }
        catch {
            $errorMsg = $_.Exception.Message
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
            else {
                $errorType = $errorMsg.Substring(0, [Math]::Min(50, $errorMsg.Length))
            }
        }
        
        [PSCustomObject]@{
            ReportName = $report.ReportName
            ReportPath = $report.ReportPath
            FileSizeKB = [Math]::Round($fileSize / 1024, 2)
            SchemaVersion = $schemaVersion
            IsPowerBI = $isPowerBI
            HasNamespace = $hasNamespace
            HasReportNode = $hasReportNode
            EncodingDecl = $encodingDecl
            HasBOM = $hasBOM
            DataSourceCount = $dataSourceCount
            DataSetCount = $dataSetCount
            ErrorType = $errorType
            FirstLine = $firstLine.Substring(0, [Math]::Min(100, $firstLine.Length))
            ModifiedDate = $report.ModifiedDate
            ModifiedBy = $report.ModifiedBy
        }
    }
    catch {
        Write-Warning "Error diagnosing $($report.ReportPath): $_"
        
        [PSCustomObject]@{
            ReportName = $report.ReportName
            ReportPath = $report.ReportPath
            FileSizeKB = 0
            SchemaVersion = "ERROR"
            IsPowerBI = $false
            HasNamespace = $false
            HasReportNode = $false
            EncodingDecl = "ERROR"
            HasBOM = $false
            DataSourceCount = 0
            DataSetCount = 0
            ErrorType = "Cannot access report"
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

# Show Power BI reports
$powerBICount = ($diagnostics | Where-Object {$_.IsPowerBI -eq $true}).Count
Write-Host "Power BI Reports: $powerBICount" -ForegroundColor $(if ($powerBICount -gt 0) {"Yellow"} else {"White"})
Write-Host ""

# Show reports with BOM issues
$bomCount = ($diagnostics | Where-Object {$_.HasBOM -eq $true}).Count
Write-Host "Reports with BOM: $bomCount" -ForegroundColor $(if ($bomCount -gt 0) {"Yellow"} else {"White"})
Write-Host ""

# Show detailed list
Write-Host "=== FAILED REPORTS DETAIL ===" -ForegroundColor Red
$diagnostics | Format-Table ReportName, SchemaVersion, ErrorType, FileSizeKB, ModifiedDate -AutoSize

Write-Host ""
Write-Host "Full diagnostics exported to: C:\Audits\FailedReports_Diagnostics.csv" -ForegroundColor Cyan
Write-Host ""

# Provide recommendations
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "RECOMMENDATIONS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$diagnostics | ForEach-Object {
    if ($_.ErrorType -eq "Invalid child node structure") {
        Write-Host "`n$($_.ReportName):" -ForegroundColor Yellow
        Write-Host "  Issue: RDL structure is non-standard or corrupted" -ForegroundColor White
        Write-Host "  Action: Try opening in Report Builder and re-saving" -ForegroundColor Gray
    }
    elseif ($_.ErrorType -eq "Truncated/incomplete XML") {
        Write-Host "`n$($_.ReportName):" -ForegroundColor Yellow
        Write-Host "  Issue: Report file may be corrupted" -ForegroundColor White
        Write-Host "  Action: Restore from backup or recreate" -ForegroundColor Gray
    }
    elseif ($_.ErrorType -eq "Namespace conflict") {
        Write-Host "`n$($_.ReportName):" -ForegroundColor Yellow
        Write-Host "  Issue: XML namespace declaration problem" -ForegroundColor White
        Write-Host "  Action: Review XML manually or re-save in Report Builder" -ForegroundColor Gray
    }
    elseif ($_.IsPowerBI) {
        Write-Host "`n$($_.ReportName):" -ForegroundColor Yellow
        Write-Host "  Issue: Power BI report (not standard RDL)" -ForegroundColor White
        Write-Host "  Action: These cannot be parsed as RDL - this is expected" -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "NEXT STEPS:" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "1. Review the CSV file for detailed analysis" -ForegroundColor White
Write-Host "2. For 'Invalid child node' errors, try re-saving in Report Builder" -ForegroundColor White
Write-Host "3. For Power BI reports, note that RDL parsing doesn't apply" -ForegroundColor White
Write-Host "4. For corrupted files, restore from backup if critical" -ForegroundColor White
Write-Host "5. If most are old/deprecated, consider archiving them" -ForegroundColor White
Write-Host ""