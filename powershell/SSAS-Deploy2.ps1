<#
.SYNOPSIS
  Deploy SSAS 2022 Tabular model from Model.bim (no VS, no Tabular Editor).

.DESCRIPTION
  - Loads Model.bim from repo
  - Patches data source connection string(s)
  - Wraps as TMSL createOrReplace targeting -SSASDatabase
  - If database exists: fails unless -Overwrite is supplied
  - Deploys via Invoke-ASCmd
  - Can be run with no parameters; prompts interactively with sensible defaults.
  - SQLServer is auto-built from Environment: sql.app.<env>.mydomain.com

.NOTES
  Requires PowerShell module: SqlServer (Invoke-ASCmd)

.EXAMPLE
  # Fully interactive (just run it and press Enter for defaults):
  .\SSAS-Deploy.ps1

.EXAMPLE
  # Fully parameterized (no prompts):
  .\SSAS-Deploy.ps1 -Environment DEV -SSASServer "myssas01" -SSASDatabase "SimAnalytics" `
    -ProjectPath "D:\repo\SimulationsAnalytics" -ModelBimRelativePath "Model.bim" `
    -DataSourceName "SimulationsAnalytics" -SQLDatabase "SimulationsAnalytics"
#>

[CmdletBinding()]
param(
  [Parameter()]
  [ValidateSet("DEV","QAT","UAT","PRD","")]
  [string]$Environment,

  [Parameter()]
  [string]$SSASServer,

  [Parameter()]
  [string]$SSASDatabase,

  [Parameter()]
  [string]$ProjectPath,

  [Parameter()]
  [string]$ModelBimRelativePath,

  [Parameter()]
  [string]$DataSourceName,

  [Parameter()]
  [string]$SQLDatabase,

  [Parameter()]
  [string]$ConnectionStringOverride,

  [Parameter()]
  [switch]$Overwrite
)

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------
# Interactive prompt helper
# ---------------------------------------------------------------
function Read-ParameterWithDefault {
  param(
    [string]$PromptText,
    [string]$Default
  )
  $displayDefault = if ($Default) { $Default } else { "(none)" }
  $input = Read-Host "$PromptText [$displayDefault]"
  if ([string]::IsNullOrWhiteSpace($input)) { return $Default }
  return $input.Trim()
}

# ---------------------------------------------------------------
# Prompt for missing parameters
# ---------------------------------------------------------------
$isInteractive = [Environment]::UserInteractive -and -not $MyInvocation.BoundParameters.ContainsKey("Environment")

# --- Environment ---
if ([string]::IsNullOrWhiteSpace($Environment)) {
  $Environment = Read-ParameterWithDefault -PromptText "Environment (DEV|QAT|UAT|PRD)" -Default "DEV"
}
$validEnvs = @("DEV","QAT","UAT","PRD")
if ($Environment -notin $validEnvs) {
  throw "Invalid Environment '$Environment'. Must be one of: $($validEnvs -join ', ')"
}

# --- SSASServer (auto-built default from Environment) ---
$envLower = $Environment.ToLower()
$ssasDefault = "sql.app.$envLower.mydomain.com"
if ([string]::IsNullOrWhiteSpace($SSASServer)) {
  $SSASServer = Read-ParameterWithDefault -PromptText "SSAS Server name" -Default $ssasDefault
}

# --- SSASDatabase ---
if ([string]::IsNullOrWhiteSpace($SSASDatabase)) {
  $SSASDatabase = Read-ParameterWithDefault -PromptText "SSAS Database name" -Default "SimulationsAnalytics"
}

# --- SQLServer (defaults to SSASServer) ---
if ([string]::IsNullOrWhiteSpace($SQLServer)) {
  $SQLServer = Read-ParameterWithDefault -PromptText "SQL Server name (data source)" -Default $SSASServer
}

# --- SQLDatabase (defaults to SSASDatabase) ---
if ([string]::IsNullOrWhiteSpace($SQLDatabase)) {
  $SQLDatabase = Read-ParameterWithDefault -PromptText "SQL Database name (data source)" -Default $SSASDatabase
}

# --- ProjectPath ---
$projectDefault = "C:\source\repos\DIV.Simulations.EDW\SimulationsAnalytics" # Location on DBT Host VM
if (-not $projectDefault) { $projectDefault = (Get-Location).Path }
if ([string]::IsNullOrWhiteSpace($ProjectPath)) {
  $ProjectPath = Read-ParameterWithDefault -PromptText "Project path (repo root)" -Default $projectDefault
}

# --- ModelBimRelativePath ---
if ([string]::IsNullOrWhiteSpace($ModelBimRelativePath)) {
  $ModelBimRelativePath = Read-ParameterWithDefault -PromptText "Model.bim relative path" -Default "Model.bim"
}

# --- DataSourceName ---
if ([string]::IsNullOrWhiteSpace($DataSourceName)) {
  $DataSourceName = Read-ParameterWithDefault -PromptText "DataSource name to patch (blank = all)" -Default "SimulationsAnalytics"
}

# --- SQLDatabase ---
if ([string]::IsNullOrWhiteSpace($SQLDatabase)) {
  $SQLDatabase = Read-ParameterWithDefault -PromptText "SQL Database name" -Default $SSASDatabase
}

# --- ConnectionStringOverride ---
if ([string]::IsNullOrWhiteSpace($ConnectionStringOverride)) {
  $csInput = Read-ParameterWithDefault -PromptText "Connection string override (blank = auto-build)" -Default ""
  if (-not [string]::IsNullOrWhiteSpace($csInput)) {
    $ConnectionStringOverride = $csInput
  }
}

# --- Overwrite ---
if (-not $MyInvocation.BoundParameters.ContainsKey("Overwrite")) {
  $owInput = Read-ParameterWithDefault -PromptText "Overwrite if exists? (Y/N)" -Default "Y"
  $Overwrite = $owInput -match '^[Yy]'
}

# ---------------------------------------------------------------
# Functions
# ---------------------------------------------------------------
function Write-Banner($msg) {
  Write-Host "==============================================================="
  Write-Host $msg
  Write-Host "==============================================================="
}

function Install-SqlServerModuleIfNeeded {
  if (-not (Get-Module -ListAvailable -Name SqlServer)) {
    Write-Host "SqlServer module not found. Installing to CurrentUser..."
    Install-Module -Name SqlServer -Force -AllowClobber -Scope CurrentUser
  }
  Import-Module SqlServer
}

function Get-VisibleCatalogNames([string]$server) {
  $discover = @'
<Discover xmlns="urn:schemas-microsoft-com:xml-analysis">
  <RequestType>DBSCHEMA_CATALOGS</RequestType>
  <Restrictions />
  <Properties />
</Discover>
'@
  $raw = Invoke-ASCmd -Server $server -Query $discover -ErrorAction Stop
  [xml]$doc = $raw

  @(
    $doc.DiscoverResponse.return.root.row |
    ForEach-Object { $_.CATALOG_NAME } |
    Where-Object { $_ -and $_.Trim() -ne "" }
  )
}

function Update-ConnectionStringParts {
  param(
    [Parameter(Mandatory)][string]$ConnectionString,
    [string]$NewServer,
    [string]$NewDatabase
  )

  $cs = $ConnectionString

  if ($NewServer) {
    $cs = [regex]::Replace($cs, '(?i)(\bData Source\s*=\s*)[^;]*', "`$1$NewServer")
    $cs = [regex]::Replace($cs, '(?i)(\bServer\s*=\s*)[^;]*', "`$1$NewServer")
    $cs = [regex]::Replace($cs, '(?i)(\bAddress\s*=\s*)[^;]*', "`$1$NewServer")
  }

  if ($NewDatabase) {
    $cs = [regex]::Replace($cs, '(?i)(\bInitial Catalog\s*=\s*)[^;]*', "`$1$NewDatabase")
    $cs = [regex]::Replace($cs, '(?i)(\bDatabase\s*=\s*)[^;]*', "`$1$NewDatabase")
  }

  return $cs
}

function Update-ModelDataSources {
  param(
    [Parameter(Mandatory)]$DatabaseObject,
    [string]$TargetDataSourceName,
    [string]$SQLServer,
    [string]$SQLDatabase,
    [string]$ConnectionStringOverride
  )

  if (-not $DatabaseObject.model -or -not $DatabaseObject.model.dataSources) {
    throw "Model.bim does not contain model.dataSources. Cannot patch connection strings."
  }

  $dataSources = $DatabaseObject.model.dataSources

  $targets =
    if ($TargetDataSourceName) {
      $dataSources | Where-Object { $_.name -eq $TargetDataSourceName }
    } else {
      $dataSources
    }

  if (-not $targets -or $targets.Count -eq 0) {
    $available = ($dataSources | ForEach-Object { $_.name }) -join ", "
    throw "No matching dataSources found to patch. Requested='$TargetDataSourceName'. Available=[$available]"
  }

  foreach ($ds in $targets) {
    if (-not $ds.connectionString) {
      throw "Datasource '$($ds.name)' does not have a connectionString property."
    }

    $old = $ds.connectionString

    if ($ConnectionStringOverride) {
      $ds.connectionString = $ConnectionStringOverride
    } else {
      if (-not $SQLServer -and -not $SQLDatabase) {
        throw "To patch connection strings, provide either -ConnectionStringOverride OR (-SQLServer and/or SQLDatabase)."
      }
      $ds.connectionString = Update-ConnectionStringParts -ConnectionString $old -NewServer $SQLServer -NewDatabase $SQLDatabase
    }

    Write-Host "Patched datasource '$($ds.name)'."
    Write-Host "  OLD: $old"
    Write-Host "  NEW: $($ds.connectionString)"
  }

  return $DatabaseObject
}

# ---------------------------------------------------------------
# Start
# ---------------------------------------------------------------
Write-Banner "SSAS TABULAR DEPLOY (Model.bim) - $Environment"
Write-Host "Timestamp        : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Host "SSAS Server      : $SSASServer"
Write-Host "SSAS Database    : $SSASDatabase"
Write-Host "ProjectPath      : $ProjectPath"
Write-Host "Model.bim (rel)  : $ModelBimRelativePath"
Write-Host "Overwrite        : $Overwrite"
Write-Host "DataSourceName   : $DataSourceName"
Write-Host "SQL Server       : $SQLServer"
Write-Host "SQL Database     : $SQLDatabase"
Write-Host "CS Override set? : $([bool]$ConnectionStringOverride)"
Write-Host ""

#exit 0 # TEMP for testing parameter parsing only

if (-not (Test-Path -LiteralPath $ProjectPath)) {
  throw "ProjectPath not found: $ProjectPath"
}

$ModelBimPath = Join-Path $ProjectPath $ModelBimRelativePath
if (-not (Test-Path -LiteralPath $ModelBimPath)) {
  throw "Model.bim not found at: $ModelBimPath"
}

Install-SqlServerModuleIfNeeded

# Connectivity + existence check
Write-Host "Testing SSAS connectivity and checking database existence..."
$catalogs = Get-VisibleCatalogNames -server $SSASServer
$exists = $catalogs -contains $SSASDatabase

if ($exists -and -not $Overwrite) {
  throw "SSAS database '$SSASDatabase' already exists on '$SSASServer'. Re-run with -Overwrite to replace it."
}
if ($exists -and $Overwrite) {
  Write-Host "Database exists and -Overwrite supplied: will replace."
}
if (-not $exists) {
  Write-Host "Database does not exist: will create."
}

# Load Model.bim
Write-Host "Loading Model.bim..."
$modelBimRaw = Get-Content -Raw -Path $ModelBimPath
$databaseObj = $modelBimRaw | ConvertFrom-Json -Depth 200

# Ensure database name matches target
$databaseObj.name = $SSASDatabase

# Patch data source connection strings
Write-Host "Patching data source connection string(s)..."
$databaseObj = Update-ModelDataSources `
  -DatabaseObject $databaseObj `
  -TargetDataSourceName $DataSourceName `
  -SQLServer $SQLServer `
  -SQLDatabase $SQLDatabase `
  -ConnectionStringOverride $ConnectionStringOverride

# Wrap in createOrReplace
Write-Host "Building TMSL createOrReplace..."
$tmslObj = [pscustomobject]@{
  createOrReplace = [pscustomobject]@{
    object   = [pscustomobject]@{ database = $SSASDatabase }
    database = $databaseObj
  }
}

$tmslJson = $tmslObj | ConvertTo-Json -Depth 200

# Deploy
Write-Host "Deploying to SSAS via Invoke-ASCmd..."
try {
  Invoke-ASCmd -Server $SSASServer -Query $tmslJson -ErrorAction Stop | Out-Null
  Write-Host "Deployment succeeded."
}
catch {
  throw "Deployment failed: $_"
}

Write-Banner "DONE"
exit 0