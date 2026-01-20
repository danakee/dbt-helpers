$ServerName  = "sql.odsitar.app.dev.flightsafety.com"
$DBName      = "SimulationsAnalytics"
$errorLogged = $false

function Get-TabularTables {
    param(
        [Parameter(Mandatory=$true)][string]$Server,
        [Parameter(Mandatory=$true)][string]$Database
    )

    # ADOMD.NET is usually installed with SSMS, Tabular Editor, or Microsoft Analysis Services libraries.
    # If this fails to load, see the note below for the fallback.
    Add-Type -AssemblyName "Microsoft.AnalysisServices.AdomdClient" -ErrorAction Stop

    $conn = New-Object Microsoft.AnalysisServices.AdomdClient.AdomdConnection
    $conn.ConnectionString = "Data Source=$Server;Catalog=$Database"

    $cmd = $conn.CreateCommand()
    $cmd.CommandText = @"
SELECT
    [Name]      AS TableName,
    [TableType] AS TableType
FROM `$SYSTEM.TMSCHEMA_TABLES
"@

    $conn.Open()
    try {
        $adapter = New-Object Microsoft.AnalysisServices.AdomdClient.AdomdDataAdapter($cmd)
        $dt = New-Object System.Data.DataTable
        [void]$adapter.Fill($dt)

        if ($dt.Rows.Count -eq 0) {
            throw "DMV returned 0 rows from TMSCHEMA_TABLES."
        }

        # Keep only physical data tables (skip Calculated tables)
        $tables =
            $dt.Rows |
            Where-Object { $_.TableType -eq "Data" } |
            ForEach-Object { [string]$_.TableName } |
            Sort-Object { if ($_ -like "Dim*") { 0 } else { 1 } }, { $_ }

        return ,$tables
    }
    finally {
        $conn.Close()
    }
}

$tables = Get-TabularTables -Server $ServerName -Database $DBName

Write-Host "Discovered $($tables.Count) data tables." -ForegroundColor Yellow
if (-not $tables -or $tables.Count -eq 0) { throw "No data tables discovered. Stopping." }

foreach ($table in $tables) {
    $start = Get-Date
    Write-Host ("[{0:HH:mm:ss}] START: {1}" -f $start, $table) -ForegroundColor Cyan

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
