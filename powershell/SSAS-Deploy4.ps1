<#
.SYNOPSIS
  Deploy SSAS 2022 Tabular model from Model.bim (no VS, no Tabular Editor).

.DESCRIPTION
  - Loads Model.bim from repo
  - Patches data source connection string(s)
  - Wraps as TMSL createOrReplace targeting -SSASDatabase
  - If database exists: fails unless -Overwrite is supplied
  - Deploys via Invoke-ASCmd
  - Optionally processes (refreshes) the deployed model
  - Can be run with no parameters; prompts interactively with sensible defaults.
  - SQLServer is auto-built from Environment: sql.app.<env>.mydomain.com

.NOTES
  Requires PowerShell module: SqlServer (Invoke-ASCmd)

.EXAMPLE
  # Fully interactive (just run it and press Enter for defaults):
  .\SSAS-Deploy.ps1

.EXAMPLE
  # Fully parameterized, deploy + full process:
  .\SSAS-Deploy.ps1 -Environment DEV -SSASServer "myssas01" -SSASDatabase "SimAnalytics" `
    -ProjectPath "D:\repo\SimulationsAnalytics" -ModelBimRelativePath "Model.bim" `
    -DataSourceName "SimulationsAnalytics" -SQLServer "myssas01" -SQLDatabase "SimulationsAnalytics" `
    -Overwrite -Process -ProcessType full

.EXAMPLE
  # Deploy + process only specific tables:
  .\SSAS-Deploy.ps1 -Environment QAT -Overwrite -Process -ProcessType full `
    -ProcessTables "DimDate,FactSimulatorConfiguration"
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
  [string]$SQLServer,

  [Parameter()]
  [string]$SQLDatabase,

  [Parameter()]
  [string]$ProjectPath,

  [Parameter()]
  [string]$ModelBimRelativePath,

  [Parameter()]
  [string]$DataSourceName,

  [Parameter()]
  [string]$ConnectionStringOverride,

  [Parameter()]
  [switch]$Overwrite,

  [Parameter()]
  [switch]$Process,

  [Parameter()]
  [ValidateSet("full","automatic","clearValues","")]
  [string]$ProcessType,

  [Parameter()]
  [string]$ProcessTables
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
  $userResponse = Read-Host "$PromptText [$displayDefault]"
  if ([string]::IsNullOrWhiteSpace($userResponse)) { return $Default }
  return $userResponse.Trim()
}

# ---------------------------------------------------------------
# Determine if running interactively
# ---------------------------------------------------------------
$isInteractive = [Environment]::UserInteractive -and -not $MyInvocation.BoundParameters.ContainsKey("Environment")

# ---------------------------------------------------------------
# Prompt for missing parameters (interactive) or validate (CI)
# ---------------------------------------------------------------

# --- Environment ---
if ([string]::IsNullOrWhiteSpace($Environment)) {
  if ($isInteractive) {
    $Environment = Read-ParameterWithDefault -PromptText "Environment (DEV|QAT|UAT|PRD)" -Default "DEV"
  } else {
    $Environment = "DEV"
  }
}
$validEnvs = @("DEV","QAT","UAT","PRD")
if ($Environment -notin $validEnvs) {
  throw "Invalid Environment '$Environment'. Must be one of: $($validEnvs -join ', ')"
}

# --- SSASServer (auto-built default from Environment) ---
$envLower = $Environment.ToLower()
$ssasDefault = "sql.app.$envLower.mydomain.com"
if ([string]::IsNullOrWhiteSpace($SSASServer)) {
  if ($isInteractive) {
    $SSASServer = Read-ParameterWithDefault -PromptText "SSAS Server name (target)" -Default $ssasDefault
  } else {
    $SSASServer = $ssasDefault
  }
}

# --- SSASDatabase ---
if ([string]::IsNullOrWhiteSpace($SSASDatabase)) {
  if ($isInteractive) {
    $SSASDatabase = Read-ParameterWithDefault -PromptText "SSAS Database name (target)" -Default "SimulationsAnalytics"
  } else {
    $SSASDatabase = "SimulationsAnalytics"
  }
}

# --- SQLServer (defaults to SSASServer) ---
if ([string]::IsNullOrWhiteSpace($SQLServer)) {
  if ($isInteractive) {
    $SQLServer = Read-ParameterWithDefault -PromptText "SQL Server name (data source)" -Default $SSASServer
  } else {
    $SQLServer = $SSASServer
  }
}

# --- SQLDatabase (defaults to SSASDatabase) ---
if ([string]::IsNullOrWhiteSpace($SQLDatabase)) {
  if ($isInteractive) {
    $SQLDatabase = Read-ParameterWithDefault -PromptText "SQL Database name (data source)" -Default $SSASDatabase
  } else {
    $SQLDatabase = $SSASDatabase
  }
}

# --- ProjectPath ---
$projectDefault = "C:\source\repos\DIV.Simulations.EDW\SimulationsAnalytics"
if (-not $projectDefault) { $projectDefault = (Get-Location).Path }
if ([string]::IsNullOrWhiteSpace($ProjectPath)) {
  if ($isInteractive) {
    $ProjectPath = Read-ParameterWithDefault -PromptText "Project path (repo root)" -Default $projectDefault
  } else {
    $ProjectPath = $projectDefault
  }
}

# --- ModelBimRelativePath ---
if ([string]::IsNullOrWhiteSpace($ModelBimRelativePath)) {
  if ($isInteractive) {
    $ModelBimRelativePath = Read-ParameterWithDefault -PromptText "Model.bim relative path" -Default "Model.bim"
  } else {
    $ModelBimRelativePath = "Model.bim"
  }
}

# --- DataSourceName ---
if ([string]::IsNullOrWhiteSpace($DataSourceName)) {
  if ($isInteractive) {
    $DataSourceName = Read-ParameterWithDefault -PromptText "DataSource name to patch (blank = all)" -Default "SimulationsAnalytics"
  } else {
    $DataSourceName = "SimulationsAnalytics"
  }
}

# --- ConnectionStringOverride ---
if ([string]::IsNullOrWhiteSpace($ConnectionStringOverride)) {
  if ($isInteractive) {
    $csInput = Read-ParameterWithDefault -PromptText "Connection string override (blank = auto-build)" -Default ""
    if (-not [string]::IsNullOrWhiteSpace($csInput)) {
      $ConnectionStringOverride = $csInput
    }
  }
}

# --- Overwrite ---
if (-not $MyInvocation.BoundParameters.ContainsKey("Overwrite")) {
  if ($isInteractive) {
    $owInput = Read-ParameterWithDefault -PromptText "Overwrite if exists? (Y/N)" -Default "Y"
    $Overwrite = $owInput -match '^[Yy]'
  } else {
    $Overwrite = $false
  }
}

# --- Process ---
if (-not $MyInvocation.BoundParameters.ContainsKey("Process")) {
  if ($isInteractive) {
    $procInput = Read-ParameterWithDefault -PromptText "Process (refresh) after deploy? (Y/N)" -Default "Y"
    $Process = $procInput -match '^[Yy]'
  } else {
    $Process = $false
  }
}

# --- ProcessType (only relevant when -Process is active) ---
if ($Process -and [string]::IsNullOrWhiteSpace($ProcessType)) {
  if ($isInteractive) {
    $ProcessType = Read-ParameterWithDefault -PromptText "Process type (full|automatic|clearValues)" -Default "full"
  } else {
    $ProcessType = "full"
  }
}
$validProcessTypes = @("full","automatic","clearValues")
if ($Process -and $ProcessType -notin $validProcessTypes) {
  throw "Invalid ProcessType '$ProcessType'. Must be one of: $($validProcessTypes -join ', ')"
}

# --- ProcessTables (only relevant when -Process is active; blank = database-level / all tables) ---
if ($Process -and [string]::IsNullOrWhiteSpace($ProcessTables)) {
  if ($isInteractive) {
    $ProcessTables = Read-ParameterWithDefault -PromptText "Tables to process (comma-separated, blank = all / database-level)" -Default ""
  }
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
        throw "To patch connection strings, provide either -ConnectionStringOverride OR (-SQLServer and/or -SQLDatabase)."
      }
      $ds.connectionString = Update-ConnectionStringParts -ConnectionString $old -NewServer $SQLServer -NewDatabase $SQLDatabase
    }

    Write-Host "Patched datasource '$($ds.name)'."
    Write-Host "  OLD: $old"
    Write-Host "  NEW: $($ds.connectionString)"
  }

  return $DatabaseObject
}

function Invoke-DatabaseRefresh {
  <#
  .SYNOPSIS
    Database-level TMSL refresh. Processes all tables in the database.
  #>
  param(
    [Parameter(Mandatory)][string]$Server,
    [Parameter(Mandatory)][string]$Database,
    [Parameter(Mandatory)][string]$Type
  )

  $tmsl = [pscustomobject]@{
    refresh = [pscustomobject]@{
      type    = $Type
      objects = @(
        [pscustomobject]@{ database = $Database }
      )
    }
  }

  $json = if ($PSVersionTable.PSVersion.Major -ge 7) {
    $tmsl | ConvertTo-Json -Depth 50
  } else {
    $tmsl | ConvertTo-Json -Depth 20
  }

  Write-Host "TMSL refresh (database-level, type=$Type):"
  Write-Host $json
  Write-Host ""

  $result = Invoke-ASCmd -Server $Server -Query $json -ErrorAction Stop
  return $result
}

function Invoke-TableRefresh {
  <#
  .SYNOPSIS
    Table-level TMSL refresh. Processes one or more specific tables.
  #>
  param(
    [Parameter(Mandatory)][string]$Server,
    [Parameter(Mandatory)][string]$Database,
    [Parameter(Mandatory)][string]$Type,
    [Parameter(Mandatory)][string[]]$Tables
  )

  $objectList = @()
  foreach ($tbl in $Tables) {
    $objectList += [pscustomobject]@{
      database = $Database
      table    = $tbl.Trim()
    }
  }

  $tmsl = [pscustomobject]@{
    refresh = [pscustomobject]@{
      type    = $Type
      objects = $objectList
    }
  }

  $json = if ($PSVersionTable.PSVersion.Major -ge 7) {
    $tmsl | ConvertTo-Json -Depth 50
  } else {
    $tmsl | ConvertTo-Json -Depth 20
  }

  Write-Host "TMSL refresh (table-level, type=$Type, tables=$($Tables -join ', ')):"
  Write-Host $json
  Write-Host ""

  $result = Invoke-ASCmd -Server $Server -Query $json -ErrorAction Stop
  return $result
}

function Test-RefreshResult {
  <#
  .SYNOPSIS
    Parses XMLA result from Invoke-ASCmd and throws on error.
  #>
  param(
    [Parameter(Mandatory)][string]$RawResult,
    [string]$Context = "Refresh"
  )

  if ([string]::IsNullOrWhiteSpace($RawResult)) {
    Write-Host "$Context returned empty result (typically success)."
    return
  }

  # Invoke-ASCmd returns XML; check for error elements
  try {
    [xml]$doc = $RawResult

    $nsManager = New-Object System.Xml.XmlNamespaceManager($doc.NameTable)
    $nsManager.AddNamespace("x", "urn:schemas-microsoft-com:xml-analysis")
    $nsManager.AddNamespace("e", "urn:schemas-microsoft-com:xml-analysis:exception")

    $errorNodes    = $doc.SelectNodes("//x:Error",  $nsManager)
    $errorNodes2   = $doc.SelectNodes("//e:Error",  $nsManager)
    $genericErrors = $doc.SelectNodes("//Error")

    $allErrors = @()
    if ($errorNodes)    { $allErrors += $errorNodes    }
    if ($errorNodes2)   { $allErrors += $errorNodes2   }
    if ($genericErrors) { $allErrors += $genericErrors }

    if ($allErrors.Count -gt 0) {
      $messages = $allErrors | ForEach-Object {
        $desc = $_.Description
        $code = $_.ErrorCode
        if ($desc) { "[$code] $desc" } else { $_.OuterXml }
      }
      throw "$Context failed with $($allErrors.Count) error(s):`n$($messages -join "`n")"
    }
  }
  catch [System.Xml.XmlException] {
    # Not valid XML — dump raw for diagnostics
    Write-Warning "$Context returned non-XML result:`n$RawResult"
  }

  Write-Host "$Context completed successfully."
}

# ---------------------------------------------------------------
# Start
# ---------------------------------------------------------------
Write-Banner "SSAS TABULAR DEPLOY (Model.bim) - $Environment"
Write-Host "Timestamp        : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Host "SSAS Server      : $SSASServer"
Write-Host "SSAS Database    : $SSASDatabase"
Write-Host "SQL Server       : $SQLServer"
Write-Host "SQL Database     : $SQLDatabase"
Write-Host "ProjectPath      : $ProjectPath"
Write-Host "Model.bim (rel)  : $ModelBimRelativePath"
Write-Host "DataSourceName   : $DataSourceName"
Write-Host "Overwrite        : $Overwrite"
Write-Host "CS Override set? : $([bool]$ConnectionStringOverride)"
Write-Host "Process          : $Process"
if ($Process) {
  Write-Host "ProcessType      : $ProcessType"
  $processScope = if ([string]::IsNullOrWhiteSpace($ProcessTables)) { "(all tables / database-level)" } else { $ProcessTables }
  Write-Host "ProcessTables    : $processScope"
}
Write-Host ""

# ---------------------------------------------------------------
# Validate paths
# ---------------------------------------------------------------
if (-not (Test-Path -LiteralPath $ProjectPath)) {
  throw "ProjectPath not found: $ProjectPath"
}

$ModelBimPath = Join-Path $ProjectPath $ModelBimRelativePath
if (-not (Test-Path -LiteralPath $ModelBimPath)) {
  throw "Model.bim not found at: $ModelBimPath"
}

Install-SqlServerModuleIfNeeded

# ---------------------------------------------------------------
# Connectivity + existence check
# ---------------------------------------------------------------
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

# ---------------------------------------------------------------
# Load Model.bim
# ---------------------------------------------------------------
# NOTE: On Windows PowerShell 5.1, ConvertFrom-Json does not support -Depth.
#       On PS 7+, -Depth 200 ensures deep nesting is preserved.
Write-Host "Loading Model.bim..."
$modelBimRaw = Get-Content -Raw -Path $ModelBimPath
if ($PSVersionTable.PSVersion.Major -ge 7) {
  $databaseObj = $modelBimRaw | ConvertFrom-Json -Depth 200
} else {
  $databaseObj = $modelBimRaw | ConvertFrom-Json
}

# Ensure database name matches target
$databaseObj.name = $SSASDatabase

# ---------------------------------------------------------------
# Patch data source connection strings
# ---------------------------------------------------------------
Write-Host "Patching data source connection string(s)..."
$databaseObj = Update-ModelDataSources `
  -DatabaseObject $databaseObj `
  -TargetDataSourceName $DataSourceName `
  -SQLServer $SQLServer `
  -SQLDatabase $SQLDatabase `
  -ConnectionStringOverride $ConnectionStringOverride

# ---------------------------------------------------------------
# Wrap in createOrReplace and deploy
# ---------------------------------------------------------------
Write-Host "Building TMSL createOrReplace..."
$tmslObj = [pscustomobject]@{
  createOrReplace = [pscustomobject]@{
    object   = [pscustomobject]@{ database = $SSASDatabase }
    database = $databaseObj
  }
}

if ($PSVersionTable.PSVersion.Major -ge 7) {
  $tmslJson = $tmslObj | ConvertTo-Json -Depth 200
} else {
  $tmslJson = $tmslObj | ConvertTo-Json -Depth 100
}

Write-Host "Deploying to SSAS via Invoke-ASCmd..."
try {
  Invoke-ASCmd -Server $SSASServer -Query $tmslJson -ErrorAction Stop | Out-Null
  Write-Host "Deployment succeeded."
}
catch {
  throw "Deployment failed: $_"
}

# ---------------------------------------------------------------
# Process (refresh) if requested
# ---------------------------------------------------------------
if ($Process) {
  Write-Banner "PROCESSING - $ProcessType"

  $tableList = @()
  if (-not [string]::IsNullOrWhiteSpace($ProcessTables)) {
    $tableList = $ProcessTables -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }
  }

  $refreshStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

  try {
    if ($tableList.Count -gt 0) {
      # Table-level refresh: one TMSL command targeting specific tables
      Write-Host "Processing $($tableList.Count) table(s): $($tableList -join ', ')"
      $rawResult = Invoke-TableRefresh `
        -Server   $SSASServer `
        -Database $SSASDatabase `
        -Type     $ProcessType `
        -Tables   $tableList
    } else {
      # Database-level refresh: processes all tables
      Write-Host "Processing entire database (all tables)..."
      $rawResult = Invoke-DatabaseRefresh `
        -Server   $SSASServer `
        -Database $SSASDatabase `
        -Type     $ProcessType
    }

    Test-RefreshResult -RawResult $rawResult -Context "Process ($ProcessType)"
  }
  catch {
    $refreshStopwatch.Stop()
    Write-Host "Processing failed after $($refreshStopwatch.Elapsed.ToString('hh\:mm\:ss'))."
    throw "Processing failed: $_"
  }

  $refreshStopwatch.Stop()
  Write-Host "Processing completed in $($refreshStopwatch.Elapsed.ToString('hh\:mm\:ss'))."
}

# ---------------------------------------------------------------
# Done
# ---------------------------------------------------------------
$doneMsg = "DONE - Deploy"
if ($Process) { $doneMsg += " + Process ($ProcessType)" }
if (-not $Process) { $doneMsg += "  (model deployed but NOT processed)" }
Write-Banner $doneMsg
exit 0