$reportServerUri = "http://your-pbirs-server/ReportServer/ReportService2010.asmx?wsdl"
$rs = New-WebServiceProxy -Uri $reportServerUri -UseDefaultCredential

# Import your audit results
$audit = Import-Csv "C:\Audits\ReportSecurityAudit.csv"

Write-Host "Audit File Analysis:" -ForegroundColor Cyan
Write-Host "Total Reports: $($audit.Count)" -ForegroundColor White
Write-Host ""

# Check for blank/null values
$blankEmbedded = $audit | Where-Object {$_.HasEmbeddedDataSources -eq ""}
$trueEmbedded = $audit | Where-Object {$_.HasEmbeddedDataSources -eq "True"}
$falseEmbedded = $audit | Where-Object {$_.HasEmbeddedDataSources -eq "False"}

Write-Host "HasEmbeddedDataSources values:" -ForegroundColor Yellow
Write-Host "  Blank/Null: $($blankEmbedded.Count)" -ForegroundColor Red
Write-Host "  True: $($trueEmbedded.Count)" -ForegroundColor White
Write-Host "  False: $($falseEmbedded.Count)" -ForegroundColor White
Write-Host ""

# Let's manually check a few reports to verify
Write-Host "Spot-checking 3 random reports..." -ForegroundColor Cyan
$samplesToCheck = $audit | Get-Random -Count 3

foreach ($sample in $samplesToCheck) {
    Write-Host "`nChecking: $($sample.ReportPath)" -ForegroundColor Yellow
    
    try {
        $reportDef = $rs.GetItemDefinition($sample.ReportPath)
        $rdl = [System.Text.Encoding]::UTF8.GetString($reportDef)
        [xml]$rdlXml = $rdl
        
        # Check data source type
        Write-Host "  Data Sources found:" -ForegroundColor White
        
        if ($rdlXml.Report.DataSources.DataSource) {
            foreach ($ds in $rdlXml.Report.DataSources.DataSource) {
                $dsName = $ds.Name
                
                if ($ds.ConnectionProperties) {
                    Write-Host "    - $dsName : EMBEDDED (ConnectionProperties)" -ForegroundColor Red
                }
                elseif ($ds.DataSourceReference) {
                    Write-Host "    - $dsName : SHARED (Reference: $($ds.DataSourceReference))" -ForegroundColor Green
                }
                else {
                    Write-Host "    - $dsName : UNKNOWN TYPE" -ForegroundColor Yellow
                }
            }
        }
        else {
            Write-Host "    - No DataSources node found" -ForegroundColor Gray
        }
        
        Write-Host "  CSV says HasEmbeddedDataSources: $($sample.HasEmbeddedDataSources)" -ForegroundColor Cyan
    }
    catch {
        Write-Warning "Error checking $($sample.ReportPath): $_"
    }
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Recommendations:" -ForegroundColor Cyan
Write-Host "1. If most values are blank, the original script had parsing issues" -ForegroundColor White
Write-Host "2. If spot-checks show embedded sources but CSV says blank/false, re-run audit" -ForegroundColor White
Write-Host "3. If spot-checks match CSV values, data is good" -ForegroundColor White