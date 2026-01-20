$ServerName  = "sql.odsitar.app.dev.flightsafety.com"
$DBName      = "SimulationsAnalytics"
$errorLogged = $false

function Get-TabularDataTablesFromDmv {
    param(
        [Parameter(Mandatory=$true)][string]$Server,
        [Parameter(Mandatory=$true)][string]$Database
    )

    # DMV query (returns XMLA rowset wrapped in <root><Messages>...)
    $dmvQuery = @"
SELECT
    [Name]      AS TableName,
    [TableType] AS TableType
FROM `$SYSTEM.TMSCHEMA_TABLES
ORDER BY [Name]
"@

    $raw = Invoke-ASCmd -Server $Server -Database $Database -Query $dmvQuery -ErrorAction Stop

    if ([string]::IsNullOrWhiteSpace($raw)) {
        throw "Invoke-ASCmd returned empty output for DMV query."
    }

    # Invoke-ASCmd typically returns an XML string for DMV rowsets. Parse as XML.
    # NOTE: Sometimes there can be leading junk/verbose text; we’ll try to locate the first '<'.
    $firstLt = $raw.IndexOf('<')
    if ($firstLt -lt 0) {
        throw "DMV output was not XML. First 200 chars: $($raw.Substring(0, [Math]::Min(200, $raw.Length)))"
    }

    $xmlText = $raw.Substring($firstLt)

    [xml]$doc = $xmlText

    # Rowset results are usually under: /root/return/row
    # But some builds emit slightly different envelopes, so we try a few.
    $rowNodes =
        @(
            $doc.SelectNodes("//return/row"),
            $doc.SelectNodes("//root/return/row"),
            $doc.SelectNodes("//row")
        ) | Where-Object { $_ -and $_.Count -gt 0 } | Select-Object -First 1

    if (-not $rowNodes -or $rowNodes.Count -eq 0) {
        # Helpful debug: dump a snippet of the XML
        $snippet = $xmlText.Substring(0, [Math]::Min(600, $xmlText.Length))
        throw "Could not find row nodes in DMV XML output. XML snippet:`n$snippet"
    }

    # Extract values. In XMLA rowsets, columns show up as child elements under <row>
    $tables = foreach ($row in $rowNodes) {
        $name = $row.TableName
        if (-not $name) { $name = $row.Name }  # fallback if aliases aren't honored
        $type = $row.TableType

        [pscustomobject]@{
            Name     = [string]$name
            TableType= [string]$type
        }
    }

    # Filter to physical data tables
    $dataTables =
        $tables |
        Where-Object { $_.Name -and $_.TableType -eq "Data" } |
        Select-Object -ExpandProperty Name -Unique |
        Sort-Object { if ($_ -like "Dim*") { 0 } else { 1 } }, { $_ }

    return ,$dataTables
}

# --- Discover tables ---
$tables = Get-TabularDataTablesFromDmv -Server $ServerName -Database $DBName
Write-Host "Discovered $($tables.Count) Data tables from DMVs." -ForegroundColor Yellow

if (-not $tables -or $tables.Count -eq 0) {
    throw "No data tables discovered. Stopping."
}

# --- Process tables one-by-one ---
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

# --- Final calc ---
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
