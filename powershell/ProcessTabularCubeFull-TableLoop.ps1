$ServerName  = "sql.odsitar.app.dev.flightsafety.com"
$DBName      = "SimulationsAnalytics"
$errorLogged = $false

# OPTIONAL: explicit ordering beats guessing.
# Put your true Dim/Fact order here if you have dependencies.
# $tables = @("DimDate","DimTime","DimUser", ... ,"FactIssueActivity")

# If you still want discovery, at least validate it returns something.
# (The DMV parsing approach varies by environment, so keep this defensive.)
# $tables = ...

foreach ($table in $tables) {
    $start = Get-Date
    Write-Host ("[{0:HH:mm:ss}] START: {1}" -f $start, $table) -ForegroundColor Cyan

    # Pick ONE refresh type for the per-table loop:
    # "full" = full process of that table/partitions
    # "dataOnly" = re-pull data only (often still fine, but depends on model)
    $tmslTable = @"
{
  "refresh": {
    "type": "full",
    "objects": [
      { "database": "$DBName", "table": "$table" }
    ]
  }
}
"@

    try {
        Invoke-ASCmd -Server $ServerName -Query $tmslTable -ErrorAction Stop | Out-Null
        $end = Get-Date
        $dur = New-TimeSpan -Start $start -End $end
        Write-Host ("[{0:HH:mm:ss}] SUCCESS: {1} ({2})" -f $end, $table, $dur) -ForegroundColor Green
    }
    catch {
        $errorLogged = $true
        Write-Host ("[{0:HH:mm:ss}] ERROR: {1}" -f (Get-Date), $table) -ForegroundColor Red
        Write-Host ("    {0}" -f $_.Exception.Message) -ForegroundColor Red
    }
}

Write-Host "--- FINAL CALC ---" -ForegroundColor Yellow
$tmslCalc = @"
{
  "refresh": {
    "type": "calculate",
    "objects": [
      { "database": "$DBName" }
    ]
  }
}
"@

Invoke-ASCmd -Server $ServerName -Query $tmslCalc -ErrorAction Stop | Out-Null

if ($errorLogged) { Write-Warning "Finished with some table-level errors." }
else { Write-Host "Full process complete." -ForegroundColor Green }
