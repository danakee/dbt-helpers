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
  If omitted, a prefix like ".\SsisEnv_<MACHINENAME>_yyyyMMdd_HHmmss"
  will be created in the current directory.

  Example: -OutputPrefix 'C:\Temp\SsisEnv_DEV01'
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
function Get-SsisInfo {
    # Be specific so we don't pick up unrelated products.
    $patterns = @(
        'SQL Server *Integration Services*'
    )

    $items = Get-InstalledProgramsLike -NamePatterns $patterns

    [PSCustomObject]@{
        SsisEntries = $items
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
function Get-SsisVsixInfo {
    # SSIS Projects usually shows as a separate installed program
    $patterns = @(
        '*SQL Server Integration Services Projects*',
        '*Integration Services Projects 2022*'
    )

    $items = Get-InstalledProgramsLike -NamePatterns $patterns

    [PSCustomObject]@{
        SsisVsixEntries = $items
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
            Instances = $null
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
            Instances = $null
        }
    }

    if (-not $json -or $json.Trim().Length -eq 0 -or $json.Trim() -eq '[]') {
        return [PSCustomObject]@{
            Found     = $false
            Message   = "vswhere.exe returned no VS 2022 instances."
            Instances = $null
        }
    }

    $instances = $json | ConvertFrom-Json
    if (-not $instances) {
        return [PSCustomObject]@{
            Found     = $false
            Message   = "vswhere JSON parse succeeded but no VS 2022 instances were found."
            Instances = $null
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

    # Mapping of Release DWORD to human-friendly version.
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
$ssisInfo     = Get-SsisInfo
$vsInfo       = Get-Vs2022Info
$ssisVsixInfo = Get-SsisVsixInfo
$vstaInfo     = Get-VstaInfo
$dotNetInfo   = Get-DotNetFramework4Info

$result = [PSCustomObject]@{
    Timestamp        = Get-Date
    Host             = $hostInfo
    SqlServer        = $sqlInfo
    SsisInstalled    = $ssisInfo.SsisEntries
    VisualStudio2022 = $vsInfo
    SsisVsix         = $ssisVsixInfo.SsisVsixEntries
    Vsta             = $vstaInfo.VstaEntries
    DotNet4          = $dotNetInfo
}

# Output to console for quick inspection
$result

#-------------------------------#
# File output (TXT + JSON + CSV)
#-------------------------------#

# If no prefix supplied, create a default one in the current directory.
if (-not $OutputPrefix -or $OutputPrefix.Trim().Length -eq 0) {
    $ts = Get-Date -Format 'yyyyMMdd_HHmmss'
    $baseName = "SsisEnv_$($hostInfo.MachineName)_$ts"
    $OutputPrefix = Join-Path (Get-Location) $baseName
}

# Build file paths
$txtPath  = "$OutputPrefix.txt"
$jsonPath = "$OutputPrefix.json"
$ssisCsv  = "$OutputPrefix_SsisInstalled.csv"
$vstaCsv  = "$OutputPrefix_Vsta.csv"
$vsCsv    = "$OutputPrefix_VS2022.csv"

# 1) JSON – full raw object
$result | ConvertTo-Json -Depth 6 | Set-Content -Path $jsonPath -Encoding UTF8

# 2) CSVs for tabular bits
if ($result.SsisInstalled) {
    $result.SsisInstalled | Export-Csv -Path $ssisCsv -NoTypeInformation -Encoding UTF8
}
if ($result.Vsta) {
    $result.Vsta | Export-Csv -Path $vstaCsv -NoTypeInformation -Encoding UTF8
}
if ($result.VisualStudio2022.Found -and $result.VisualStudio2022.Instances) {
    $result.VisualStudio2022.Instances | Export-Csv -Path $vsCsv -NoTypeInformation -Encoding UTF8
}

# 3) Human-readable TXT summary
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
if ($result.SsisInstalled) {
    $lines += ($result.SsisInstalled | Format-Table DisplayName,DisplayVersion,Publisher -AutoSize | Out-String).TrimEnd()
} else {
    $lines += "(none found)"
}
$lines += ""

$lines += "---- Visual Studio 2022 ----"
if ($result.VisualStudio2022.Found -and $result.VisualStudio2022.Instances) {
    $lines += ($result.VisualStudio2022.Instances |
               Format-Table installationName,installationVersion,installationPath,ProductDisplayName -AutoSize |
               Out-String).TrimEnd()
} else {
    $lines += "Found : $($result.VisualStudio2022.Found)"
    $lines += "Message: $($result.VisualStudio2022.Message)"
}
$lines += ""

$lines += "---- SSIS Projects Extension (VSIX) ----"
if ($result.SsisVsix) {
    $lines += ($result.SsisVsix |
               Format-Table DisplayName,DisplayVersion,Publisher -AutoSize |
               Out-String).TrimEnd()
} else {
    $lines += "(none found)"
}
$lines += ""

$lines += "---- VSTA (Visual Studio Tools for Applications) ----"
if ($result.Vsta) {
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

Write-Host "Results written to:" -ForegroundColor Green
Write-Host "  $txtPath"
Write-Host "  $jsonPath"
if (Test-Path $ssisCsv) { Write-Host "  $ssisCsv" }
if (Test-Path $vstaCsv) { Write-Host "  $vstaCsv" }
if (Test-Path $vsCsv)   { Write-Host "  $vsCsv" }
