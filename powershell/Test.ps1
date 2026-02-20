$ErrorActionPreference = "Stop"

$deploy = Join-Path $PSScriptRoot "Pipelines\cube\SSAS-Deploy.ps1"

$env          = "DEV"
$ssasServer   = "sql.odisitar.app.dev.flightsafety.com"   # adjust
$ssasDatabase = "SimulationsAnalytics_Tabular"            # adjust
$projectPath  = "D:\ADO\_work\1\s\SimulationsAnalytics"   # adjust
$modelRel     = "Pipelines\cube\Model.bim"                # adjust

# Patch target (either use server+db OR full CS override)
$sqlServer    = "edw-dev01"
$sqlDatabase  = "SimulationsAnalytics"

& $deploy `
  -Environment $env `
  -SsasServer $ssasServer `
  -SsasDatabase $ssasDatabase `
  -ProjectPath $projectPath `
  -ModelBimRelativePath $modelRel `
  -SqlServerName $sqlServer `
  -SqlDatabaseName $sqlDatabase `
  -Overwrite