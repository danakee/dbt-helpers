<#  Compare selected stored procedures across three servers with Redgate SQL Compare
    File format expected at $inputList:
        spLoadOPAStage_BMD_ActivityType
        spLoadOPAStage_BMD_StaticProgram
        ... (more procs; schema optional, defaults to dbo)

        sql.operationsanalytics.app.uat.flightsafety.com
        sql.operationsanalytics.app.qat.flightsafety.com
        sql.operationsanalytics.app.dev.flightsafety.com

    To Execute the script from power shell:

        Set-Location C:\Diffs
        Set-ExecutionPolicy -Scope Process RemoteSigned
        Unblock-File .\Compare-Procedures.ps1
        .\Compare-Procedures.ps1

#>

# --------------------- SETTINGS ---------------------
$SqlCompareExe = "C:\Program Files (x86)\Red Gate\SQL Compare 15\sqlcompare.exe"
$inputList     = "C:\Diffs\ProcAndServers.txt"   # list file with stored procedures and server names
$outDir        = "C:\Diffs\Reports"
$db            = "OperationsAnalyticsStage"

# Comparison options (adjust as needed)
$opts = "IgnoreWhitespace,IgnoreComments"

# Optional: also write the include switches to a file for debugging
$writeDebugIncludes = $true

# --------------------- READ & PARSE INPUT ---------------------
# Read lines as-is to find the first blank separator
$raw = Get-Content $inputList -ErrorAction Stop

# Find first truly blank line (separator between procs and servers)
$splitIndex = $null
for ($i=0; $i -lt $raw.Count; $i++) {
    if ($raw[$i] -match '^\s*$') { $splitIndex = $i; break }
}
if ($null -eq $splitIndex) {
    throw "Expected a blank line separating procedures and servers in $inputList."
}

# Procedures = lines before blank; Servers = lines after blank (skip extra blanks/comments)
$procs = $raw[0..($splitIndex-1)] |
         ForEach-Object { $_.Trim() } |
         Where-Object { $_ -and $_ -notmatch '^\s*#' }

$servers = $raw[($splitIndex+1)..($raw.Count-1)] |
           ForEach-Object { $_.Trim() } |
           Where-Object { $_ -and $_ -notmatch '^\s*#' }

if ($servers.Count -ne 3) { throw "Expected 3 server names after the blank line; found $($servers.Count)." }
$serverA,$serverB,$serverC = $servers

# --------------------- HELPERS ---------------------
function Parse-ProcName([string]$name) {
    # Returns an object with Schema, Proc, and Full ("schema.proc"), defaulting schema to dbo
    $t = $name.Trim()
    if ($t -match '\.') {
        $parts = $t -split '\.', 2
        $schema = $parts[0]
        $proc   = $parts[1]
    } else {
        $schema = 'dbo'
        $proc   = $t
    }
    [pscustomobject]@{
        Schema  = $schema
        Proc    = $proc
        Full    = "$schema.$proc"
    }
}

function To-IncludeSwitches([string[]]$names) {
    foreach ($n in $names) {
        if (-not $n -or $n -match '^\s*#') { continue }
        $p = Parse-ProcName $n
        # Escape metacharacters and build anchored regex for fully-qualified, bracketed name
        $schemaEsc = [Regex]::Escape($p.Schema)
        $procEsc   = [Regex]::Escape($p.Proc)
        "/Include:StoredProcedure:^\[$schemaEsc\]\.\[$procEsc\]$"
    }
}

function Run-Compare($s1,$db1,$s2,$db2,$reportName,[string[]]$includes) {
    $args = @(
        "/server1:$s1", "/db1:$db1",
        "/server2:$s2", "/db2:$db2",
        "/report:`"$($outDir)\$reportName`"",
        "/reportType:Html",
        "/Options:$opts"
    ) + $includes

    & $SqlCompareExe @args
    if ($LASTEXITCODE -ne 0) {
        Write-Host "⚠️  SQL Compare exit code $LASTEXITCODE for $reportName" -ForegroundColor Yellow
    } else {
        Write-Host "✅  Created $reportName"
    }
}

# --------------------- BUILD INCLUDES & SHOW INPUTS ---------------------
# Normalize procs for display and for include switches
$procObjs    = $procs | ForEach-Object { Parse-ProcName $_ }
$includeArgs = To-IncludeSwitches $procs

# Output directory (also used for optional debug file)
New-Item -ItemType Directory -Path $outDir -Force | Out-Null
if ($writeDebugIncludes) {
    $includeArgs | Set-Content -Path (Join-Path $outDir "debug_includes.txt")
}

# ---- DISPLAY SERVERS & PROCS ONCE BEFORE RUNS ----
Write-Host ""
Write-Host "🔎 Inputs detected from $inputList" -ForegroundColor Cyan
Write-Host "`nServers & databases:" -ForegroundColor White
@(
    [pscustomobject]@{ Order = "A"; Server = $serverA; Database = $db }
    [pscustomobject]@{ Order = "B"; Server = $serverB; Database = $db }
    [pscustomobject]@{ Order = "C"; Server = $serverC; Database = $db }
) | Format-Table -AutoSize | Out-String | Write-Host

Write-Host ("Stored procedures to compare ({0}):" -f $procObjs.Count) -ForegroundColor White
$procObjs | ForEach-Object { Write-Host ("  - {0}" -f $_.Full) }
if ($writeDebugIncludes) {
    Write-Host ("(Include patterns written to {0}\debug_includes.txt)" -f $outDir) -ForegroundColor DarkGray
}
Write-Host ""

# --------------------- DATABASE NAMES ---------------------
# If the DB name differs per server, set these three explicitly instead of using $db for all.
$dbA = $db
$dbB = $db
$dbC = $db

# --------------------- RUN THE THREE COMPARES ---------------------
Run-Compare $serverA $dbA $serverB $dbB "A_vs_B_Procs.html" $includeArgs
Run-Compare $serverA $dbA $serverC $dbC "A_vs_C_Procs.html" $includeArgs
Run-Compare $serverB $dbB $serverC $dbC "B_vs_C_Procs.html" $includeArgs

Write-Host "`nDone. Open the HTML files in $outDir." -ForegroundColor Cyan
