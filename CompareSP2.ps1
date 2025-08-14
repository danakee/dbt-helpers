# Compare selected stored procedures across three servers using Redgate SQL Compare
# Save as C:\Diffs\Compare-Procs.ps1 and run in PowerShell

$SqlCompareExe = "C:\Program Files (x86)\Red Gate\SQL Compare 15\sqlcompare.exe"
$inputList     = "C:\Diffs\ProcAndServers.txt"   # your file: procs, blank line, then 3 server names
$outDir        = "C:\Diffs\Reports"
$db            = "OperationsAnalytics"           # <-- change to your database name

# Comparison options (tweak as desired)
$opts = "IgnoreWhitespace,IgnoreComments"

# ---- helper: parse the file you showed (procs first, blank line, then 3 servers) ----
$lines = Get-Content $inputList | Where-Object { $_ -ne $null } | ForEach-Object { $_.Trim() }

# Split into two blocks on the first blank line (or, if none, assume last 3 are servers)
$blankIx = ($lines | ForEach-Object {$i=0} { if ($_ -eq "") { $script:i } $i++ }) | Select-Object -First 1
if ($blankIx -ne $null) {
    $procs   = $lines[0..($blankIx-1)] | Where-Object { $_ -and $_ -notmatch '^\s*#' }
    $servers = $lines[($blankIx+1)..($lines.Count-1)] | Where-Object { $_ }
} else {
    $procs   = $lines[0..($lines.Count-4)]
    $servers = $lines[($lines.Count-3)..($lines.Count-1)]
}

if ($servers.Count -ne 3) { throw "Expected 3 server names; found $($servers.Count). Please check $inputList." }

$serverA,$serverB,$serverC = $servers

# ---- helper: turn names into /Include switches (defaults schema to dbo if omitted) ----
function To-IncludeSwitches([string[]]$names) {
    foreach ($n in $names) {
        if (-not $n) { continue }
        if ($n -match '^\s*#') { continue }
        $schema,$proc = ($n -split '\.',2)
        if (-not $proc) { $schema='dbo'; $proc=$schema } # quick swap if only proc given
        if ($proc -eq $schema) { $schema='dbo' }         # normalize when only proc supplied
        "/Include:StoredProcedure:[{0}].[{1}]" -f $schema,$proc
    }
}

$includeArgs = To-IncludeSwitches $procs

# ---- runner ----
New-Item -ItemType Directory -Path $outDir -Force | Out-Null

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

# If all three servers use the same DB name:
$dbA=$db; $dbB=$db; $dbC=$db

# --- do the three pairwise comparisons ---
Run-Compare $serverA $dbA $serverB $dbB "A_vs_B_Procs.html" $includeArgs
Run-Compare $serverA $dbA $serverC $dbC "A_vs_C_Procs.html" $includeArgs
Run-Compare $serverB $dbB $serverC $dbC "B_vs_C_Procs.html" $includeArgs

Write-Host "`nDone. Open the HTML files in $outDir." -ForegroundColor Cyan
