param(
    [string] $RootPath = 'C:\',                       # Default: entire C drive
    [string] $LogDir   = 'C:\Temp',                   # Default log directory
    [string] $LogName  = "SecretScan_$((Get-Date).ToString('yyyyMMdd_HHmmss')).csv"
)

# Resolve script directory and worker path
$scriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$workerPath  = Join-Path $scriptDir 'Scan-Secrets.ps1'

if (-not (Test-Path -LiteralPath $workerPath)) {
    Write-Error "Worker script not found at: $workerPath"
    exit 1
}

# Ensure log directory exists
if (-not (Test-Path -LiteralPath $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

$logPath = Join-Path $LogDir $LogName

Write-Host "Running secret scan worker..."
Write-Host "Root path : $RootPath"
Write-Host "Log file  : $logPath"
Write-Host ""

# Call worker script
& $workerPath -RootPath $RootPath -LogPath $logPath
