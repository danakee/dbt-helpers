<#
.SYNOPSIS
  Deploy SSAS 2022 Tabular model from Model.bim (no VS, no Tabular Editor).

.DESCRIPTION
  - Loads Model.bim from repo
  - Patches data source connection string(s) — supports both legacy (connectionString)
    and structured (connectionDetails.address) datasource types
  - For structured datasources: removes any embedded Username from the credential
    block so SSAS uses its own service identity, and rebuilds credential.path
    to match the patched server/database
  - Wraps as TMSL createOrReplace targeting -SSASDatabase
  - If database exists: fails unless -Overwrite is supplied
  - Deploys via Invoke-ASCmd
  - Optionally processes (refreshes) the deployed model
  - Optionally exports the TMSL/XMLA to disk (with or without deploying)
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
    -DataSourceName "SQL/SimulationsAnalytics" -SQLServer "myssas01" -SQLDatabase "SimulationsAnalytics" `
    -Overwrite -Process -ProcessType full

.EXAMPLE
  # Deploy + process only specific tables:
  .\SSAS-Deploy.ps1 -Environment QAT -Overwrite -Process -ProcessType full `
    -ProcessTables "DimDate,FactSimulatorConfiguration"

.EXAMPLE
  # Dry-run: export XMLA only, no deploy or process:
  .\SSAS-Deploy.ps1 -Environment DEV -ExportXmla -ExportOnly `
    -ExportPath "C:\temp\ssas-xmla"

.EXAMPLE
  # Deploy + process + save XMLA copies for audit:
  .\SSAS-Deploy.ps1 -Environment PRD -Overwrite -Process -ProcessType full `
    -ExportXmla -ExportPath "C:\temp\ssas-xmla"
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
  [string]$ProcessTables,

  [Parameter()]
  [switch]$ExportXmla,

  [Parameter()]
  [switch]$ExportOnly,

  [Parameter()]
  [string]$ExportPath
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
    $DataSourceName = Read-ParameterWithDefault -PromptText "DataSource name to patch (blank = all)" -Default "SQL/SimulationsAnalytics"
  } else {
    $DataSourceName = "SQL/SimulationsAnalytics"
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

# --- ExportXmla / ExportOnly / ExportPath ---
# -ExportOnly implies -ExportXmla
if ($ExportOnly) { $ExportXmla = $true }

if (-not $MyInvocation.BoundParameters.ContainsKey("ExportXmla") -and -not $ExportOnly) {
  if ($isInteractive) {
    $expInput = Read-ParameterWithDefault -PromptText "Export XMLA to disk? (Y/N)" -Default "N"
    $ExportXmla = $expInput -match '^[Yy]'
    if ($ExportXmla) {
      $eoInput = Read-ParameterWithDefault -PromptText "Export ONLY (dry-run, skip deploy)? (Y/N)" -Default "N"
      $ExportOnly = $eoInput -match '^[Yy]'
    }
  }
}

if ($ExportXmla -and [string]::IsNullOrWhiteSpace($ExportPath)) {
  $exportDefault = Join-Path $ProjectPath "xmla-export"
  if ($isInteractive) {
    $ExportPath = Read-ParameterWithDefault -PromptText "Export directory" -Default $exportDefault
  } else {
    $ExportPath = $exportDefault
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

function Update-StructuredCredential {
  <#
  .SYNOPSIS
    Patches the credential block on a structured datasource:
    - Removes Username (so SSAS uses its own service identity)
    - Rebuilds path to match patched server/database
    - Preserves AuthenticationKind, kind, EncryptConnection, etc.
  #>
  param(
    [Parameter(Mandatory)]$DataSourceObject,
    [string]$PatchedServer,
    [string]$PatchedDatabase
  )

  if (-not $DataSourceObject.PSObject.Properties['credential']) {
    Write-Host "  No credential block found — nothing to patch."
    return
  }

  $cred = $DataSourceObject.credential

  # --- Remove Username so SSAS falls back to its service identity ---
  if ($cred.PSObject.Properties['Username']) {
    $oldUser = $cred.Username
    $cred.PSObject.Properties.Remove('Username')
    Write-Host "  Removed credential.Username (was: $oldUser)"
    Write-Host "  SSAS will use its own service account identity for data source connections."
  }

  # --- Also remove Password if present (shouldn't be in .bim, but just in case) ---
  if ($cred.PSObject.Properties['Password']) {
    $cred.PSObject.Properties.Remove('Password')
    Write-Host "  Removed credential.Password (should not be in source control)."
  }

  # --- Rebuild path to match patched server;database ---
  if ($cred.PSObject.Properties['path']) {
    $oldPath = $cred.path

    # Determine the current server/database for the new path
    $pathServer   = $PatchedServer
    $pathDatabase = $PatchedDatabase

    # If either wasn't patched, try to preserve the original from the path
    if (-not $pathServer -or -not $pathDatabase) {
      $pathParts = $oldPath -split ';', 2
      if (-not $pathServer   -and $pathParts.Count -ge 1) { $pathServer   = $pathParts[0] }
      if (-not $pathDatabase -and $pathParts.Count -ge 2) { $pathDatabase = $pathParts[1] }
    }

    $newPath = "$pathServer;$pathDatabase"
    $cred.path = $newPath
    Write-Host "  Patched credential.path:"
    Write-Host "    OLD: $oldPath"
    Write-Host "    NEW: $newPath"
  }
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
    # -------------------------------------------------------
    # Determine datasource type and patch accordingly
    # -------------------------------------------------------
    $dsType = $null

    if ($ds.PSObject.Properties['connectionString'] -and $ds.connectionString) {
      $dsType = "legacy"
    }
    elseif ($ds.PSObject.Properties['connectionDetails'] -and $ds.connectionDetails) {
      $dsType = "structured"
    }
    else {
      $propsAvailable = ($ds.PSObject.Properties | ForEach-Object { $_.Name }) -join ", "
      throw "Datasource '$($ds.name)' has neither 'connectionString' nor 'connectionDetails'. Cannot determine type. Available properties: [$propsAvailable]"
    }

    Write-Host "Datasource '$($ds.name)' detected as: $dsType"

    # ------- LEGACY datasource (flat connectionString) -------
    if ($dsType -eq "legacy") {
      $old = $ds.connectionString

      if ($ConnectionStringOverride) {
        $ds.connectionString = $ConnectionStringOverride
      } else {
        if (-not $SQLServer -and -not $SQLDatabase) {
          throw "To patch connection strings, provide either -ConnectionStringOverride OR (-SQLServer and/or -SQLDatabase)."
        }
        $ds.connectionString = Update-ConnectionStringParts -ConnectionString $old -NewServer $SQLServer -NewDatabase $SQLDatabase
      }

      Write-Host "  Patched connectionString:"
      Write-Host "    OLD: $old"
      Write-Host "    NEW: $($ds.connectionString)"
    }

    # ------- STRUCTURED datasource (connectionDetails.address) -------
    if ($dsType -eq "structured") {
      if ($ConnectionStringOverride) {
        Write-Warning "  -ConnectionStringOverride is not applicable to structured datasources. Ignoring override for '$($ds.name)'."
        Write-Warning "  Use -SQLServer / -SQLDatabase to patch structured datasources."
      }

      $address = $null
      if ($ds.connectionDetails.PSObject.Properties['address']) {
        $address = $ds.connectionDetails.address
      }

      if (-not $address) {
        throw "Structured datasource '$($ds.name)' has connectionDetails but no address object."
      }

      # Patch server
      if ($SQLServer -and $address.PSObject.Properties['server']) {
        $oldServer = $address.server
        $address.server = $SQLServer
        Write-Host "  Patched address.server:"
        Write-Host "    OLD: $oldServer"
        Write-Host "    NEW: $($address.server)"
      }
      elseif ($SQLServer) {
        $address | Add-Member -NotePropertyName 'server' -NotePropertyValue $SQLServer -Force
        Write-Host "  Added address.server: $SQLServer"
      }

      # Patch database
      if ($SQLDatabase -and $address.PSObject.Properties['database']) {
        $oldDatabase = $address.database
        $address.database = $SQLDatabase
        Write-Host "  Patched address.database:"
        Write-Host "    OLD: $oldDatabase"
        Write-Host "    NEW: $($address.database)"
      }
      elseif ($SQLDatabase) {
        $address | Add-Member -NotePropertyName 'database' -NotePropertyValue $SQLDatabase -Force
        Write-Host "  Added address.database: $SQLDatabase"
      }

      if (-not $SQLServer -and -not $SQLDatabase) {
        Write-Warning "  No -SQLServer or -SQLDatabase provided. Structured datasource '$($ds.name)' left unchanged."
      }

      # Patch credential block (remove Username, rebuild path)
      # Use the final patched values from address for path rebuild
      $finalServer   = if ($address.PSObject.Properties['server'])   { $address.server }   else { $null }
      $finalDatabase = if ($address.PSObject.Properties['database']) { $address.database } else { $null }

      Update-StructuredCredential `
        -DataSourceObject $ds `
        -PatchedServer $finalServer `
        -PatchedDatabase $finalDatabase
    }
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

function Build-RefreshJson {
  <#
  .SYNOPSIS
    Builds the TMSL refresh JSON without executing it. Used for export.
  #>
  param(
    [Parameter(Mandatory)][string]$Database,
    [Parameter(Mandatory)][string]$Type,
    [string[]]$Tables
  )

  if ($Tables -and $Tables.Count -gt 0) {
    $objectList = @()
    foreach ($tbl in $Tables) {
      $objectList += [pscustomobject]@{
        database = $Database
        table    = $tbl.Trim()
      }
    }
  } else {
    $objectList = @(
      [pscustomobject]@{ database = $Database }
    )
  }

  $tmsl = [pscustomobject]@{
    refresh = [pscustomobject]@{
      type    = $Type
      objects = $objectList
    }
  }

  if ($PSVersionTable.PSVersion.Major -ge 7) {
    return ($tmsl | ConvertTo-Json -Depth 50)
  } else {
    return ($tmsl | ConvertTo-Json -Depth 20)
  }
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
    Write-Warning "$Context returned non-XML result:`n$RawResult"
  }

  Write-Host "$Context completed successfully."
}

function Export-XmlaFile {
  <#
  .SYNOPSIS
    Writes TMSL JSON to a timestamped file in the export directory.
  #>
  param(
    [Parameter(Mandatory)][string]$Directory,
    [Parameter(Mandatory)][string]$BaseName,
    [Parameter(Mandatory)][string]$JsonContent
  )

  if (-not (Test-Path -LiteralPath $Directory)) {
    New-Item -ItemType Directory -Path $Directory -Force | Out-Null
    Write-Host "Created export directory: $Directory"
  }

  $timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
  $fileName  = "${BaseName}_${timestamp}.xmla.json"
  $filePath  = Join-Path $Directory $fileName

  Set-Content -Path $filePath -Value $JsonContent -Encoding UTF8
  Write-Host "Exported: $filePath"

  return $filePath
}

# ---------------------------------------------------------------
# Start
# ---------------------------------------------------------------
$modeLabel = if ($ExportOnly) { "EXPORT-ONLY (dry-run)" } else { "DEPLOY" }
Write-Banner "SSAS TABULAR $modeLabel (Model.bim) - $Environment"
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
Write-Host "ExportXmla       : $ExportXmla"
Write-Host "ExportOnly       : $ExportOnly"
if ($ExportXmla) {
  Write-Host "ExportPath       : $ExportPath"
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

# SqlServer module is only needed when actually deploying/processing
if (-not $ExportOnly) {
  Install-SqlServerModuleIfNeeded
}

# ---------------------------------------------------------------
# Connectivity + existence check (skip in export-only mode)
# ---------------------------------------------------------------
if (-not $ExportOnly) {
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
} else {
  Write-Host "Export-only mode: skipping SSAS connectivity check."
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
# Patch data source connection strings and credentials
# ---------------------------------------------------------------
Write-Host "Patching data source connection string(s) and credentials..."
$databaseObj = Update-ModelDataSources `
  -DatabaseObject $databaseObj `
  -TargetDataSourceName $DataSourceName `
  -SQLServer $SQLServer `
  -SQLDatabase $SQLDatabase `
  -ConnectionStringOverride $ConnectionStringOverride

# ---------------------------------------------------------------
# Wrap in createOrReplace
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

# ---------------------------------------------------------------
# Export deploy XMLA (if requested)
# ---------------------------------------------------------------
if ($ExportXmla) {
  Export-XmlaFile -Directory $ExportPath -BaseName "createOrReplace_$SSASDatabase" -JsonContent $tmslJson
}

# ---------------------------------------------------------------
# Deploy (skip in export-only mode)
# ---------------------------------------------------------------
if (-not $ExportOnly) {
  Write-Host "Deploying to SSAS via Invoke-ASCmd..."
  try {
    Invoke-ASCmd -Server $SSASServer -Query $tmslJson -ErrorAction Stop | Out-Null
    Write-Host "Deployment succeeded."
  }
  catch {
    throw "Deployment failed: $_"
  }
} else {
  Write-Host "Export-only mode: skipping deployment."
}

# ---------------------------------------------------------------
# Process (refresh) if requested
# ---------------------------------------------------------------
if ($Process) {
  $tableList = @()
  if (-not [string]::IsNullOrWhiteSpace($ProcessTables)) {
    $tableList = $ProcessTables -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }
  }

  # Export refresh XMLA (if requested)
  if ($ExportXmla) {
    $refreshJson = Build-RefreshJson -Database $SSASDatabase -Type $ProcessType -Tables $tableList
    Export-XmlaFile -Directory $ExportPath -BaseName "refresh_$SSASDatabase" -JsonContent $refreshJson
  }

  # Execute refresh (skip in export-only mode)
  if (-not $ExportOnly) {
    Write-Banner "PROCESSING - $ProcessType"

    $refreshStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

    try {
      if ($tableList.Count -gt 0) {
        Write-Host "Processing $($tableList.Count) table(s): $($tableList -join ', ')"
        $rawResult = Invoke-TableRefresh `
          -Server   $SSASServer `
          -Database $SSASDatabase `
          -Type     $ProcessType `
          -Tables   $tableList
      } else {
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
  } else {
    Write-Host "Export-only mode: skipping processing."
  }
}

# ---------------------------------------------------------------
# Done
# ---------------------------------------------------------------
$doneMsg = "DONE"
if ($ExportOnly) {
  $doneMsg += " - Export only (dry-run)"
} else {
  $doneMsg += " - Deploy"
  if ($Process) { $doneMsg += " + Process ($ProcessType)" }
  if (-not $Process) { $doneMsg += "  (model deployed but NOT processed)" }
}
if ($ExportXmla) { $doneMsg += " | XMLA exported to: $ExportPath" }
Write-Banner $doneMsg
exit 0