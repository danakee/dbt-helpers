<#
.SYNOPSIS
  Collects environment information relevant to SSIS Script Tasks.

.DESCRIPTION
  Run this on:
    - SQL Server boxes (to capture engine + SSIS runtime info)
    - Developer workstations (to capture VS2022 + SSIS extension + VSTA info)

  Outputs:
    - Console object
    - A text summary (.txt)
    - JSON file with full object
    - CSVs for SSIS, VSTA, and VS 2022 instances

.PARAMETER SqlInstance
  SQL Server instance name (or "localhost" on dev machines).

.PARAMETER OutputPrefix
  Optional base path/prefix for output files.
  If omitted, a prefix like ".\SSISEnv_<MACHINENAME>_yyyyMMdd_HHmmss"
  will be created in the current directory.
#>

param(
    [string]$SqlInstance = "localhost",
    [string]$OutputPrefix
)

Write-Host "=== Collecting SSIS / VS / VSTA environment info on $env:COMPUTERNAME ===" -ForegroundColor Cyan

#-------------------------#
# Helper: Get SQL version #
#-------------------------#
function Get-SqlServerVersion {
    param(
        [string]$Instance = "localhost"
    )

    $result = [PSCustomObject]@{
        Instance         = $Instance
        SqlServerVersion = $null
        Error            = $null
    }

    try {
        $connectionString = "Server=$Instance;Database=master;Integrated Security=SSPI;"
        $connection = New-Object System.Data.SqlClient.SqlConnection $connectionString
        $connection.Open()
        $cmd = $connection.CreateCommand()
        $cmd.CommandText = 'SELECT @@VERSION AS SqlServerVersion;'
        $reader = $cmd.ExecuteReader()
        if ($reader.Read()) {
            $result.SqlServerVersion = $reader["SqlServerVersion"]
        }
        $reader.Close()
        $connection.Close()
    }
    catch {
        $result.Error = $_.Exception.Message
    }

    return $result
}

#-------------------------------------------#
# Helper: Get "installed programs" matches #
#-------------------------------------------#
function Get-InstalledProgramsLike {
    param(
        [string[]]$NamePatterns
    )

    $paths = @(
        'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*',
        'HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*'
    )

    $items = Get-ItemProperty -Path $paths -ErrorAction SilentlyContinue |
        Where-Object {
            $n = $_.DisplayName
            $n -and ($NamePatterns | ForEach-Object { $n -like $_ }) -contains $true
        } |
        Select-Object DisplayName, DisplayVersion, Publisher, InstallLocation

    return $items
}

#------------------------------#
# Helper: Get SSIS components  #
#------------------------------#
function Get-SSISInfo {
    # Be specific so we don't pick up unrelated products.
    $patterns = @(
        'SQL Server *Integration Services*'
    )

    $items = Get-InstalledProgramsLike -NamePatterns $patterns

    [PSCustomObject]@{
        SSISEntries = $items
    }
}

#-----------------------------------------#
# Helper: Get VSTA (Visual Studio Tools) #
#-----------------------------------------#
function Get-VstaInfo {
    $patterns = @('*Visual Studio Tools for Applications*')

    $items = Get-InstalledProgramsLike -NamePatterns $patterns

    [PSCustomObject]@{
        VstaEntries = $items
    }
}

#---------------------------------------#
# Helper: Get SSIS VSIX (extension)     #
#---------------------------------------#
function Get-SSISVsixInfo {
    $patterns = @(
        '*SQL Server Integration Services Projects*',
        '*Integration Services Projects 2022*'
    )

    $items = Get-InstalledProgramsLike -NamePatterns $patterns

    [PSCustomObject]@{
        SSISVsixEntries = $items
    }
}

#------------------------------#
# Helper: Get VS 2022 info     #
#------------------------------#
function Get-Vs2022Info {
    $vswherePath = Join-Path "${env:ProgramFiles(x86)}" "Microsoft Visual Studio\Installer\vswhere.exe"

    if (-not (Test-Path $vswherePath)) {
        return [PSCustomObject]@{
            Found     = $false
            Message   = "vswhere.exe not found. Visual Studio 2022 may not be installed."
            Instances = @()
        }
    }

    try {
        $json = & $vswherePath `
            -all `
            -prerelease `
            -products * `
            -format json `
            -version "[17.0,18.0)" 2>$null
    }
    catch {
        return [PSCustomObject]@{
            Found     = $false
            Message   = "vswhere.exe failed: $($_.Exception.Message)"
            Instances = @()
        }
    }

    if (-not $json -or $json.Trim().Length -eq 0 -or $json.Trim() -eq '[]') {
        return [PSCustomObject]@{
            Found     = $false
            Message   = "vswhere.exe returned no VS 2022 instances."
            Instances = @()
        }
    }

    $instances = $json | ConvertFrom-Json
    if (-not $instances) {
        return [PSCustomObject]@{
            Found     = $false
            Message   = "vswhere JSON parse succeeded but no VS 2022 instances were found."
            Instances = @()
        }
    }

    # Normalize to an array even if only one instance
    if ($instances -isnot [System.Collections.IEnumerable] -or $instances -is [string]) {
        $instances = @($instances)
    }

    $simplified = $instances | Select-Object `
        instanceId,
        installationName,
        installationVersion,
        installationPath,
        @{Name='ProductDisplayName';Expression={ $_.productDisplayName }},
        @{Name='Channel';Expression={ $_.channelId }},
        @{Name='IsPrerelease';Expression={ $_.isPrerelease }}

    return [PSCustomObject]@{
        Found     = $true
        Instances = $simplified
    }
}

#---------------------------------#
# Helper: Get .NET 4.x info       #
#---------------------------------#
function Get-DotNetFramework4Info {
    $regPath = 'HKLM:\SOFTWARE\Microsoft\NET Framework Setup\NDP\v4\Full'
    if (-not (Test-Path $regPath)) {
        return [PSCustomObject]@{
            Found   = $false
            Release = $null
            Version = $null
            Message = ".NET Framework 4.x Full key not found."
        }
    }

    $props   = Get-ItemProperty -Path $regPath -ErrorAction SilentlyContinue
    $release = $props.Release

    $map = @{
        378389 = '4.5'
        378675 = '4.5.1'
        378758 = '4.5.1'
        379893 = '4.5.2'
        393295 = '4.6'
        393297 = '4.6'
        394254 = '4.6.1'
        394271 = '4.6.1'
        394802 = '4.6.2'
        394806 = '4.6.2'
        460798 = '4.7'
        460805 = '4.7'
        461308 = '4.7.1'
        461310 = '4.7.1'
        461808 = '4.7.2'
        461814 = '4.7.2'
        528040 = '4.8'
        533320 = '4.8.1'
        533325 = '4.8.1'
    }

    $version = $map[$release]
    if (-not $version) { $version = "Unknown (Release=$release)" }

    return [PSCustomObject]@{
        Found   = $true
        Release = $release
        Version = $version
    }
}

#-------------------------------#
# Helper: General OS / machine  #
#-------------------------------#
function Get-HostInfo {
    $osBitness = if ([Environment]::Is64BitOperatingSystem) { "64-bit" } else { "32-bit" }

    [PSCustomObject]@{
        MachineName = $env:COMPUTERNAME
        UserName    = $env:USERNAME
        OSVersion   = [System.Environment]::OSVersion.VersionString
        OSBitness   = $osBitness
        PowerShell  = $PSVersionTable.PSVersion.ToString()
    }
}

#-------------------------------#
# Main collection               #
#-------------------------------#

$hostInfo     = Get-HostInfo
$sqlInfo      = Get-SqlServerVersion -Instance $SqlInstance
$ssisInfo     = Get-SSISInfo
$vsInfo       = Get-Vs2022Info
$ssisVsixInfo = Get-SSISVsixInfo
$vstaInfo     = Get-VstaInfo
$dotNetInfo   = Get-DotNetFramework4Info

$result = [PSCustomObject]@{
    Timestamp        = Get-Date
    Host             = $hostInfo
    SqlServer        = $sqlInfo
    SSISInstalled    = $ssisInfo.SSISEntries
    VisualStudio2022 = $vsInfo
    SSISVsix         = $ssisVsixInfo.SSISVsixEntries
    Vsta             = $vstaInfo.VstaEntries
    DotNet4          = $dotNetInfo
}

# Show summary object in console
$result

#-------------------------------#
# File output (TXT + JSON + CSV)
#-------------------------------#

# (Optional) clean up any stray .csv file that might be left from older versions/tests
Remove-Item ".csv" -ErrorAction SilentlyContinue

# Default prefix in current directory if none supplied
if (-not $OutputPrefix -or $OutputPrefix.Trim().Length -eq 0) {
    $ts = Get-Date -Format 'yyyyMMdd_HHmmss'
    $baseName = "SSISEnv_$($hostInfo.MachineName)_$ts"
    $OutputPrefix = Join-Path (Get-Location) $baseName
}

$txtPath  = "${OutputPrefix}.txt"
$jsonPath = "${OutputPrefix}.json"
$ssisCsv  = "${OutputPrefix}_SSISInstalled.csv"
$vstaCsv  = "${OutputPrefix}_Vsta.csv"
$vsCsv    = "${OutputPrefix}_VS2022.csv"

# 1) JSON
$result | ConvertTo-Json -Depth 6 | Set-Content -Path $jsonPath -Encoding UTF8

# 2) CSVs
if ($result.SSISInstalled -and $result.SSISInstalled.Count -gt 0) {
    $result.SSISInstalled | Export-Csv -Path $ssisCsv -NoTypeInformation -Encoding UTF8
}
if ($result.Vsta -and $result.Vsta.Count -gt 0) {
    $result.Vsta | Export-Csv -Path $vstaCsv -NoTypeInformation -Encoding UTF8
}
if ($result.VisualStudio2022.Found -and $result.VisualStudio2022.Instances.Count -gt 0) {
    $result.VisualStudio2022.Instances |
        Export-Csv -Path $vsCsv -NoTypeInformation -Encoding UTF8
}

# 3) Human-readable TXT
$lines = @()

$lines += "=== SSIS / VS / VSTA Environment Info ==="
$lines += "Timestamp : $($result.Timestamp)"
$lines += ""
$lines += "---- Host ----"
$lines += "Machine     : $($result.Host.MachineName)"
$lines += "User        : $($result.Host.UserName)"
$lines += "OS          : $($result.Host.OSVersion)"
$lines += "Bitness     : $($result.Host.OSBitness)"
$lines += "PowerShell  : $($result.Host.PowerShell)"
$lines += ""
$lines += "---- SQL Server ----"
$lines += "Instance    : $($result.SqlServer.Instance)"
$lines += "Error       : $($result.SqlServer.Error)"
$lines += ""
$lines += "SqlVersion  :"
$lines += ($result.SqlServer.SqlServerVersion | Out-String).TrimEnd()
$lines += ""

$lines += "---- SSIS Installed ----"
if ($result.SSISInstalled -and $result.SSISInstalled.Count -gt 0) {
    $lines += ($result.SSISInstalled |
               Format-Table DisplayName,DisplayVersion,Publisher -AutoSize |
               Out-String).TrimEnd()
} else {
    $lines += "(none found)"
}
$lines += ""

$lines += "---- Visual Studio 2022 ----"
if ($result.VisualStudio2022.Found -and $result.VisualStudio2022.Instances.Count -gt 0) {
    $lines += ($result.VisualStudio2022.Instances |
               Format-Table installationName,installationVersion,installationPath,ProductDisplayName -AutoSize |
               Out-String).TrimEnd()
} else {
    $lines += "Found  : $($result.VisualStudio2022.Found)"
    $lines += "Message: $($result.VisualStudio2022.Message)"
}
$lines += ""

$lines += "---- SSIS Projects Extension (VSIX) ----"
if ($result.SSISVsix) {
    $lines += ($result.SSISVsix |
               Format-Table DisplayName,DisplayVersion,Publisher -AutoSize |
               Out-String).TrimEnd()
} else {
    $lines += "(none found)"
}
$lines += ""

$lines += "---- VSTA (Visual Studio Tools for Applications) ----"
if ($result.Vsta -and $result.Vsta.Count -gt 0) {
    $lines += ($result.Vsta |
               Format-Table DisplayName,DisplayVersion,Publisher -AutoSize |
               Out-String).TrimEnd()
} else {
    $lines += "(none found)"
}
$lines += ""

$lines += "---- .NET Framework 4.x ----"
$lines += "Found   : $($result.DotNet4.Found)"
$lines += "Release : $($result.DotNet4.Release)"
$lines += "Version : $($result.DotNet4.Version)"

$lines -join [Environment]::NewLine | Set-Content -Path $txtPath -Encoding UTF8

# 4) Summarize created files (explicit labels)
Write-Host "Results written by Collect-SSISEnvironmentInfo.ps1:" -ForegroundColor Green
if (Test-Path $txtPath)  { Write-Host "  TXT : $txtPath" }
if (Test-Path $jsonPath) { Write-Host "  JSON: $jsonPath" }
if (Test-Path $ssisCsv)  { Write-Host "  CSV : $ssisCsv (SSIS installed)" }
if (Test-Path $vstaCsv)  { Write-Host "  CSV : $vstaCsv (VSTA)" }
if (Test-Path $vsCsv)    { Write-Host "  CSV : $vsCsv (VS 2022 instances)" }
