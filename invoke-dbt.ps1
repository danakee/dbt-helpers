<# 
  invoke-dbt.ps1
  ----------------
  - Shows environment/banner info (Python/dbt adapter versions, timestamp, etc.)
  - Requires an already-activated venv
  - Normal path: forwards all arguments to `dbt` unchanged
  - Optional path (-DropModels): resolves models from explicit names and/or dbt selectors,
    then calls `dbt run-operation drop_model_tables` with proper JSON args.
#>

[CmdletBinding()]
param(
  [switch]   $DropModels,                # trigger the run-operation path
  [string[]] $Models,                    # explicit model names (comma/newline/array ok)
  [string[]] $Selectors,                 # dbt selectors, e.g. 'tag:dim', 'path:models/mart'
  [string[]] $Excludes,                  # dbt --exclude selectors
  [string]   $PackageName = "",          # optional; empty string -> macro won't filter by package
  [switch]   $ConfirmDrop                # must be present to actually drop; otherwise dry-run
)

# ---- Require an activated virtual environment (same as your script) ----
if (-not (Test-Path Env:VIRTUAL_ENV)) {
  Write-Host "Error: No virtual environment is activated. Please activate your environment first." -ForegroundColor Red
  exit 1
}

# ---- Build a banner-friendly "command" string for env + logs ----
$full_command = "dbt $args"
if ($DropModels.IsPresent) {
  # This will be replaced later with the exact run-operation + JSON we emit.
  $full_command = "dbt run-operation drop_model_tables (pending selector expansion)"
}

# ---- Timestamp ----
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

# ---- Version sniffing ----
$python_version = (python --version 2>&1).ToString().Split()[1]

$dbt_fabric_version = (pip show dbt-fabric 2>$null | Select-String -Pattern "Version:").ToString().Split()[-1]
if (-not $dbt_fabric_version) { $dbt_fabric_version = "Not installed" }

$dbt_sqlserver_version = (pip show dbt-sqlserver 2>$null | Select-String -Pattern "Version:").ToString().Split()[-1]
if (-not $dbt_sqlserver_version) { $dbt_sqlserver_version = "Not installed" }

# ---- Env vars for banner/debug (same names you showed) ----
$env:DBT_COMMAND_LINE       = $full_command
$env:DBT_FABRIC_VERSION     = $dbt_fabric_version
$env:DBT_PYTHON_VERSION     = $python_version
$env:DBT_SQLSERVER_VERSION  = $dbt_sqlserver_version
$env:DBT_EXECUTION_TIMESTAMP= $timestamp
$env:PYTHONIOENCODING       = "utf-8"

# ---- Banner ----
Write-Host "DBT_COMMAND_LINE : $env:DBT_COMMAND_LINE"
Write-Host "DBT_PYTHON_VERSION: $env:DBT_PYTHON_VERSION"
Write-Host "DBT_FABRIC_VERSION: $env:DBT_FABRIC_VERSION"
Write-Host "DBT_SQLSERVER_VERSION: $env:DBT_SQLSERVER_VERSION"
Write-Host "DBT_EXECUTION_TIMESTAMP: $env:DBT_EXECUTION_TIMESTAMP"
Write-Host "DBT_TARGET_SQLSERVER: $env:DBT_TARGET_SQLSERVER"

try {
  if ($DropModels.IsPresent) {

    # 1) Normalize explicit model list (comma/newline/array all OK)
    $resolved = @()
    if ($Models) {
      if ($Models.Count -eq 1 -and $Models[0] -match '[,\r\n]') {
        foreach ($m in ($Models[0] -split '[,\r\n]')) {
          $t = $m.Trim(); if ($t) { $resolved += $t }
        }
      } else {
        foreach ($m in $Models) { $t = $m.Trim(); if ($t) { $resolved += $t } }
      }
    }

    # 2) Expand selectors via dbt ls
    if ($Selectors) {
      $lsArgs = @('ls','--resource-type','model','--output','name','--select')
      $lsArgs += $Selectors
      if ($Excludes -and $Excludes.Count -gt 0) { $lsArgs += @('--exclude'); $lsArgs += $Excludes }

      Write-Host "Resolving selectors via: dbt $($lsArgs -join ' ')"
      $listed = & dbt @lsArgs | Where-Object { $_ -and $_.Trim() -ne '' }
      foreach ($n in $listed) { $resolved += $n.Trim() }
    }

    # 3) De-duplicate
    $resolved = $resolved | Sort-Object -Unique

    if ($resolved.Count -eq 0) {
      Write-Error "No models resolved. Provide -Models and/or -Selectors."
      exit 1
    }

    Write-Host "Resolved models to drop:"
    $resolved | ForEach-Object { Write-Host "  - $_" }

    # 4) Safety gate: only drop when confirmed
    if ($ConfirmDrop.IsPresent) {
      $env:ALLOW_TABLE_DROP = '1'
    } else {
      $env:ALLOW_TABLE_DROP = '0'
      Write-Host "ConfirmDrop not set; refusing to drop. (Dry-run above)"
      return
    }

    # 5) Build JSON for --args (empty string package_name is fine; macro ignores it)
    $argsObj = [ordered]@{
      models       = $resolved
      package_name = $PackageName
      confirm      = $true
    }
    $argsJson = $argsObj | ConvertTo-Json -Depth 5 -Compress

    # 6) Update banner with the exact run-operation we’re about to execute
    $env:DBT_COMMAND_LINE = "dbt run-operation drop_model_tables --args $argsJson"
    Write-Host "Invoking: $env:DBT_COMMAND_LINE"

    # 7) Execute drop
    dbt run-operation drop_model_tables --args $argsJson
    return
  }

  $isRun = $args -contains "run"
  & dbt @args

  if ($LASTEXITCODE -eq 0 -and $isRun) {
    Write-Host "Running show_run_results_html.py..."
    python show_run_results_html.py
  }
}
catch {
  Write-Error "Error running dbt: $_"
}
finally {
  # ---- Clean up env variables (same pattern you had) ----
  Remove-Item Env:DBT_COMMAND_LINE -ErrorAction SilentlyContinue
  Remove-Item Env:DBT_PYTHON_VERSION -ErrorAction SilentlyContinue
  Remove-Item Env:DBT_FABRIC_VERSION -ErrorAction SilentlyContinue
  Remove-Item Env:DBT_SQLSERVER_VERSION -ErrorAction SilentlyContinue
  Remove-Item Env:DBT_EXECUTION_TIMESTAMP -ErrorAction SilentlyContinue
  Remove-Item Env:PYTHONIOENCODING -ErrorAction SilentlyContinue
  Remove-Item Env:ALLOW_TABLE_DROP -ErrorAction SilentlyContinue
}
