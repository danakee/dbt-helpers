<#
.SYNOPSIS
  Deploy SSAS 2022 Tabular model from Model.bim (no VS, no Tabular Editor).

.DESCRIPTION
  - Loads Model.bim from repo
  - Patches data source connection string(s)
  - Wraps as TMSL createOrReplace targeting -SsasDatabase
  - If database exists: fails unless -Overwrite is supplied
  - Deploys via Invoke-ASCmd

.NOTES
  Requires PowerShell module: SqlServer (Invoke-ASCmd)

.EXAMPLE
  .\SSAS-Deploy.ps1 -Environment DEV -SsasServer "myssas01" -SsasDatabase "SimAnalytics" `
    -ProjectPath "D:\repo\SimulationsAnalytics" -ModelBimRelativePath "Cube\Model.bim" `
    -DataSourceName "SimulationsAnalytics" -SqlServerName "edw-dev01" -SqlDatabaseName "SimulationsAnalytics"
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory)]
  [ValidateSet("DEV","QAT","UAT","PRD")]
  [string]$Environment,

  [Parameter(Mandatory)]
  [ValidateNotNullOrEmpty()]
  [string]$SsasServer,         # server or server\instance

  [Parameter(Mandatory)]
  [ValidateNotNullOrEmpty()]
  [string]$SsasDatabase,       # SSAS TABULAR database name (catalog)

  [Parameter(Mandatory)]
  [ValidateNotNullOrEmpty()]
  [string]$ProjectPath,        # repo root or folder where Model.bim is reachable

  [Parameter()]
  [ValidateNotNullOrEmpty()]
  [string]$ModelBimRelativePath = "Model.bim",

  # Patch strategy:
  # - If DataSourceName provided: patch only that datasource
  # - Else: patch ALL datasources found in Model.bim
  [Parameter()]
  [string]$DataSourceName,

  # Patch target:
  # Provide either (SqlServerName + SqlDatabaseName) OR a full ConnectionStringOverride.
  [Parameter()]
  [string]$SqlServerName,

  [Parameter()]
  [string]$SqlDatabaseName,

  [Parameter()]
  [string]$ConnectionStringOverride,

  [Parameter()]
  [switch]$Overwrite
)

$ErrorActionPreference = "Stop"

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
    # Replace Data Source=... or Server=...
    $cs = [regex]::Replace($cs, '(?i)(\bData Source\s*=\s*)[^;]*', "`$1$NewServer")
    $cs = [regex]::Replace($cs, '(?i)(\bServer\s*=\s*)[^;]*', "`$1$NewServer")
    $cs = [regex]::Replace($cs, '(?i)(\bAddress\s*=\s*)[^;]*', "`$1$NewServer")
  }

  if ($NewDatabase) {
    # Replace Initial Catalog=... or Database=...
    $cs = [regex]::Replace($cs, '(?i)(\bInitial Catalog\s*=\s*)[^;]*', "`$1$NewDatabase")
    $cs = [regex]::Replace($cs, '(?i)(\bDatabase\s*=\s*)[^;]*', "`$1$NewDatabase")
  }

  return $cs
}

function Update-ModelDataSources {
  param(
    [Parameter(Mandatory)]$DatabaseObject, # deserialized Model.bim root (database object)
    [string]$TargetDataSourceName,
    [string]$SqlServerName,
    [string]$SqlDatabaseName,
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
      if (-not $SqlServerName -and -not $SqlDatabaseName) {
        throw "To patch connection strings, provide either -ConnectionStringOverride OR (-SqlServerName and/or -SqlDatabaseName)."
      }
      $ds.connectionString = Update-ConnectionStringParts -ConnectionString $old -NewServer $SqlServerName -NewDatabase $SqlDatabaseName
    }

    Write-Host "Patched datasource '$($ds.name)'."
    Write-Host "  OLD: $old"
    Write-Host "  NEW: $($ds.connectionString)"
  }

  return $DatabaseObject
}

# ---------------------------
# Start
# ---------------------------
Write-Banner "SSAS TABULAR DEPLOY (Model.bim) - $Environment"
Write-Host "Timestamp       : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Host "SSAS Server      : $SsasServer"
Write-Host "SSAS Database    : $SsasDatabase"
Write-Host "ProjectPath      : $ProjectPath"
Write-Host "Model.bim (rel)  : $ModelBimRelativePath"
Write-Host "Overwrite        : $Overwrite"
Write-Host "DataSourceName   : $DataSourceName"
Write-Host "SqlServerName    : $SqlServerName"
Write-Host "SqlDatabaseName  : $SqlDatabaseName"
Write-Host "CS Override set? : $([bool]$ConnectionStringOverride)"
Write-Host ""

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
$catalogs = Get-VisibleCatalogNames -server $SsasServer
$exists = $catalogs -contains $SsasDatabase

if ($exists -and -not $Overwrite) {
  throw "SSAS database '$SsasDatabase' already exists on '$SsasServer'. Re-run with -Overwrite to replace it."
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

# Ensure database name matches target (important!)
$databaseObj.name = $SsasDatabase

# Patch data source connection strings
Write-Host "Patching data source connection string(s)..."
$databaseObj = Update-ModelDataSources `
  -DatabaseObject $databaseObj `
  -TargetDataSourceName $DataSourceName `
  -SqlServerName $SqlServerName `
  -SqlDatabaseName $SqlDatabaseName `
  -ConnectionStringOverride $ConnectionStringOverride

# Wrap in createOrReplace
Write-Host "Building TMSL createOrReplace..."
$tmslObj = [pscustomobject]@{
  createOrReplace = [pscustomobject]@{
    object   = [pscustomobject]@{ database = $SsasDatabase }
    database = $databaseObj
  }
}

$tmslJson = $tmslObj | ConvertTo-Json -Depth 200

# Deploy
Write-Host "Deploying to SSAS via Invoke-ASCmd..."
try {
  Invoke-ASCmd -Server $SsasServer -Query $tmslJson -ErrorAction Stop | Out-Null
  Write-Host "Deployment succeeded."
}
catch {
  throw "Deployment failed: $_"
}

Write-Banner "DONE"
exit 0