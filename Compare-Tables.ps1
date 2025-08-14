<#  Compare selected TABLES across three servers with Redgate SQL Compare
    File format expected at $inputList:
        dbo.MyTable
        MyOtherTable             # schema optional; defaults to dbo
        ...

        serverA.domain.tld
        serverB.domain.tld
        serverC.domain.tld
#>

# --------------------- SETTINGS ---------------------
$SqlCompareExe = "C:\Program Files (x86)\Red Gate\SQL Compare 15\sqlcompare.exe"
$inputList     = "C:\Diffs\TablesAndServers.txt"   # your list file
$outDir        = "C:\Diffs\Reports"
$db            = "OperationsAnalyticsStage"

# Comparison options (table diffs rarely need whitespace/comments ignores, but add any you like)
# Examples you may consider later: IgnorePermissions, IgnoreFillFactor, IgnoreIndexes, IgnoreCollations
$opts = ""  # leave blank for none, or e.g. "IgnorePermissions,IgnoreFillFactor"

# Optional: also write the include switches to a file for debugging
$writeDebugIncludes = $true

# --------------------- READ & PARSE INPUT ---------------------
$raw = Get-Content $inputList -ErrorAction Stop

# Find first truly blank line (separator between table list and servers)
$splitIndex = $null
for ($i=0; $i -lt $raw.Count; $i++) {
    if ($raw[$i] -match '^\s*$') { $splitIndex = $i; break }
}
if ($null -eq $splitIndex) {
    throw "Expected a blank line separating tables and servers in $inputList."
}

# Tables = lines before blank; Servers = lines after blank (skip extra blanks/comments)
$tables = $raw[0..($splitIndex-1)] |
          ForEach-Object { $_.Trim() } |
          Where-Object { $_ -and $_ -notmatch '^\s*#' }

$servers = $raw[($splitIndex+1)..($raw.Count-1)] |
           ForEach-Object { $_.Trim() } |
           Where-Object { $_ -and $_ -notmatch '^\s*#' }

if ($servers.Count -ne 3) { throw "Expected 3 server names after the blank line; found $($servers.Count)." }
$serverA,$serverB,$serverC = $servers

# --------------------- HELPERS ---------------------
function Parse-ObjectName([string]$name) {
    # Returns Schema, Name, Full ("schema.name"); defaults schema to dbo
    $t = $name.Trim()
    if ($t -match '\.') {
        $parts = $t -split '\.', 2
        $schema = $parts[0]
        $obj    = $parts[1]
    } else {
        $schema = 'dbo'
        $obj    = $t
    }
    [pscustomobject]@{
        Schema = $schema
        Name   = $obj
        Full   = "$schema.$obj"
    }
}

function To-IncludeSwitches([string[]]$names) {
    foreach ($n in $names) {
        if (-not $n -or $n -match '^\s*#') { continue }
        $p = Parse-ObjectName $n
        # Escape metacharacters and build anchored regex for fully-qualified, bracketed name
        $schemaEsc = [Regex]::Escape($p.Schema)
        $nameEsc   = [Regex]::Escape($p.Name)
        "/Include:Table:^\[$schemaEsc\]\.\[$nameEsc\]$"
    }
}

function Run-Compare($s1,$db1,$s2,$db2,$reportName,[string[]]$includes) {
    $args = @(
        "/server1:$s1", "/db1:$db1",
        "/server2:$s2", "/db2:$db2",
        "/report:`"$($outDir)\$reportName`"",
        "/reportType:Html"
    )
    if ($opts) { $args += "/Options:$opts" }
    $args += $includes

    & $SqlCompareExe @args
    if ($LASTEXITCODE -ne 0) {
        Write-Host "⚠️  SQL Compare exit code $LASTEXITCODE for $reportName" -ForegroundColor Yellow
    } else {
        Write-Host "✅  Created $reportName"
    }
}

# --------------------- BUILD INCLUDES & SHOW INPUTS ---------------------
# Normalize table names for display and for include switches
$tableObjs   = $tables | ForEach-Object { Parse-ObjectName $_ }
$includeArgs = To-IncludeSwitches $tables

# Output directory (also used for optional debug file)
New-Item -ItemType Directory -Path $outDir -Force | Out-Null
if ($writeDebugIncludes) {
    $includeArgs | Set-Content -Path (Join-Path $outDir "debug_includes_tables.txt")
}

# ---- DISPLAY SERVERS & TABLES ONCE BEFORE RUNS ----
Write-Host ""
Write-Host "🔎 Inputs detected from $inputList" -ForegroundColor Cyan
Write-Host "`nServers & databases:" -ForegroundColor White
@(
    [pscustomobject]@{ Order = "A"; Server = $serverA; Database = $db }
    [pscustomobject]@{ Order = "B"; Server = $serverB; Database = $db }
    [pscustomobject]@{ Order = "C"; Server = $serverC; Database = $db }
) | Format-Table -AutoSize | Out-String | Write-Host

Write-Host ("Tables to compare ({0}):" -f $tableObjs.Count) -ForegroundColor White
$tableObjs | ForEach-Object { Write-Host ("  - {0}" -f $_.Full) }
if ($writeDebugIncludes) {
    Write-Host ("(Include patterns written to {0}\debug_includes_tables.txt)" -f $outDir) -ForegroundColor DarkGray
}
Write-Host ""

# --------------------- DATABASE NAMES ---------------------
# If the DB name differs per server, set these three explicitly instead of using $db for all.
$dbA = $db
$dbB = $db
$dbC = $db

# --------------------- RUN THE THREE COMPARES ---------------------
Run-Compare $serverA $dbA $serverB $dbB "UAT_vs_QAT_Tables.html" $includeArgs
Run-Compare $serverA $dbA $serverC $dbC "UAT_vs_DEV_Tables.html" $includeArgs
Run-Compare $serverB $dbB $serverC $dbC "QAT_vs_DEV_Tables.html" $includeArgs

Write-Host "`nDone. Open the HTML files in $outDir." -ForegroundColor Cyan
