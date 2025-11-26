<#
.SYNOPSIS
  Collects environment information relevant to SSIS Script Tasks.

.DESCRIPTION
  Run this on:
    - SQL Server boxes (to capture engine + SSIS runtime info)
    - Developer workstations (to capture VS2022 + SSIS extension + VSTA info)

  Outputs a PowerShell object to the console and (optionally) to JSON/CSV files.

.PARAMETER SqlInstance
  SQL Server instance name or connection string target.
  Default: "localhost"

.PARAMETER OutputPrefix
  Optional base path/prefix for JSON/CSV output files.
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
        Instance          = $Instance
        SqlServerVersion  = $null
        Error             = $null
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
    # Look for Integration Services / SSIS items in installed programs.
    $patterns = @(
        '*Integration Services*',
        '*SSIS*'
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
    # The SSIS Projects extension usually shows as a separate installed program
    # like "SQL Server Integration Services Projects 2022".
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
            Found        = $false
            Message      = "vswhere.exe not found. Visual Studio 2022 may not be installed."
            RawResult    = $null
        }
    }

    $json = & $vswherePath -version "[17.0,18.0)" -products * -format json 2>$null
    if (-not $json) {
        return [PSCustomObject]@{
            Found        = $false
            Message      = "No VS 2022 instances returned by vswhere."
            RawResult    = $null
        }
    }

    $instances = $json | ConvertFrom-Json
    # Typically you’ll only have one VS 2022; keep them all just in case.
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

    $props = Get-ItemProperty -Path $regPath -ErrorAction SilentlyContinue
    $release = $props.Release

    # Basic mapping of Release DWORD to human friendly version.
    # (Not exhaustive, but good enough for common modern versions.)
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

$hostInfo        = Get-HostInfo
$sqlInfo         = Get-SqlServerVersion -Instance $SqlInstance
$ssisInfo        = Get-SsisInfo
$vsInfo          = Get-Vs2022Info
$ssisVsixInfo    = Get-SsisVsixInfo
$vstaInfo        = Get-VstaInfo
$dotNetInfo      = Get-DotNetFramework4Info

$result = [PSCustomObject]@{
    Timestamp         = (Get-Date)
    Host              = $hostInfo
    SqlServer         = $sqlInfo
    SsisInstalled     = $ssisInfo.SsisEntries
    VisualStudio2022  = $vsInfo
    SsisVsix          = $ssisVsixInfo.SsisVsixEntries
    Vsta              = $vstaInfo.VstaEntries
    DotNet4           = $dotNetInfo
}

# Output to console for quick inspection
$result

# Optional: write to files if OutputPrefix is provided
if ($OutputPrefix) {
    $jsonPath = "$OutputPrefix.json"
    $csvPath  = "$OutputPrefix_SsisInstalled.csv"
    $vstaCsv  = "$OutputPrefix_Vsta.csv"
    $vsCsv    = "$OutputPrefix_VS2022.csv"

    $result | ConvertTo-Json -Depth 6 | Set-Content -Path $jsonPath -Encoding UTF8

    if ($result.SsisInstalled) {
        $result.SsisInstalled | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
    }
    if ($result.Vsta) {
        $result.Vsta | Export-Csv -Path $vstaCsv -NoTypeInformation -Encoding UTF8
    }
    if ($result.VisualStudio2022.Found -and $result.VisualStudio2022.Instances) {
        $result.VisualStudio2022.Instances | Export-Csv -Path $vsCsv -NoTypeInformation -Encoding UTF8
    }

    Write-Host "Results written to:" -ForegroundColor Green
    Write-Host "  $jsonPath"
    if (Test-Path $csvPath) { Write-Host "  $csvPath" }
    if (Test-Path $vstaCsv) { Write-Host "  $vstaCsv" }
    if (Test-Path $vsCsv)   { Write-Host "  $vsCsv" }
}
