<#
Invoke-dbt.ps1
- Normal mode: forwards everything after the script to dbt unchanged (e.g., --version, run, test)
- Drop mode (-DropModels): resolves models (explicit + selectors) and runs drop_model_tables
- FK Operations mode: handles foreign key metadata operations
#>

[CmdletBinding()]
param(
    # Catch-all: ANYTHING not bound to a named param goes to dbt (e.g., --version, run, --threads)
    [Parameter(ValueFromRemainingArguments = $true, Position = 0)]
    [string[]] $DbtArgs,

    # Ad-hoc drop mode (opt-in)
    [switch] $DropModels,               # trigger run-operation path
    [string[]] $Models,                 # explicit model names (comma/newline/array OK)
    [string[]] $Selectors,              # dbt selectors, e.g., 'tag:dim', 'path:models/mart'
    [string[]] $Excludes,               # dbt --exclude selectors
    [string] $PackageName = "",         # empty -> macro won't filter by package
    [switch] $ConfirmDrop,              # must be present to actually drop; otherwise dry-run

    # FK Operations mode (new)
    [string] $FKOperation = "",         # preview_fk_metadata_changes, insert_fk_metadata, etc.
    [string] $FKTable = "",             # table name for FK operations
    [string[]] $FKExcludeTables = @(),  # tables to exclude
    [string[]] $FKExcludeKeys = @(),    # FK keys to exclude
    [switch] $FKPreview,                # shortcut for preview operation
    [switch] $FKInsert,                 # shortcut for insert operation
    [switch] $FKHealth                  # shortcut for health check
)

$ErrorActionPreference = 'Stop'
$script:DbtExitCode = 0

# Require an activated venv
if (-not (Test-Path Env:VIRTUAL_ENV)) {
    Write-Host "Error: No virtual environment is activated. Please activate your environment first." -ForegroundColor Red
    exit 1
}

# Banner-friendly command
$full_command = "dbt " + ($DbtArgs -join ' ')
if ($DropModels.IsPresent) {
    $full_command = "dbt run-operation drop_model_tables (pending selector expansion)"
}
if ($FKOperation -or $FKPreview -or $FKInsert -or $FKHealth) {
    $full_command = "dbt FK metadata operations"
}

# Timestamp + versions
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$python_version     = ((python --version 2>&1) -join ' ').ToString().Split()[1]
$dbt_fabric_version = (pip show dbt-fabric 2>$null | Select-String -Pattern "Version:").ToString().Split()[-1]
if (-not $dbt_fabric_version) { $dbt_fabric_version = "Not installed" }
$dbt_sqlserver_version = (pip show dbt-sqlserver 2>$null | Select-String -Pattern "Version:").ToString().Split()[-1]
if (-not $dbt_sqlserver_version) { $dbt_sqlserver_version = "Not installed" }

# Env for banner/debug
$env:DBT_COMMAND_LINE        = $full_command
$env:DBT_FABRIC_VERSION      = $dbt_fabric_version
$env:DBT_PYTHON_VERSION      = $python_version
$env:DBT_SQLSERVER_VERSION   = $dbt_sqlserver_version
$env:DBT_EXECUTION_TIMESTAMP = $timestamp
$env:PYTHONIOENCODING        = "utf-8"

# Banner
Write-Host "DBT_COMMAND_LINE: $env:DBT_COMMAND_LINE"
Write-Host "DBT_PYTHON_VERSION: $env:DBT_PYTHON_VERSION"
Write-Host "DBT_FABRIC_VERSION: $env:DBT_FABRIC_VERSION"
Write-Host "DBT_SQLSERVER_VERSION: $env:DBT_SQLSERVER_VERSION"
Write-Host "DBT_EXECUTION_TIMESTAMP: $env:DBT_EXECUTION_TIMESTAMP"
Write-Host "DBT_TARGET_SQLSERVER: $env:DBT_TARGET_SQLSERVER"

try {
    # -------------------------------
    # FK Operations mode (NEW)
    # -------------------------------
    if ($FKOperation -or $FKPreview -or $FKInsert -or $FKHealth) {
        Write-Host "Running FK metadata operations..." -ForegroundColor Cyan
        
        # Handle shortcut switches
        if ($FKPreview -and $FKTable) {
            $FKOperation = "preview_fk_metadata_changes"
        }
        elseif ($FKInsert -and $FKTable) {
            $FKOperation = "insert_fk_metadata_selective"
        }
        elseif ($FKHealth) {
            Write-Host "Running FK health check..." -ForegroundColor Yellow
            & dbt run --models fk_health_check @DbtArgs
            $script:DbtExitCode = ($LASTEXITCODE -as [int])
            return
        }
        
        if (-not $FKOperation) {
            Write-Error "FK operation not specified. Use -FKOperation, -FKPreview, -FKInsert, or -FKHealth"
            $script:DbtExitCode = 1
            return
        }
        
        # Build args for FK operations
        $fkArgsList = @()
        
        if ($FKTable) {
            $fkArgsList += "referencing_table: `"$FKTable`""
        }
        
        if ($FKExcludeTables.Count -gt 0) {
            $excludeTablesJson = "[" + (($FKExcludeTables | ForEach-Object { "`"$_`"" }) -join ", ") + "]"
            $fkArgsList += "exclude_tables: $excludeTablesJson"
        }
        
        if ($FKExcludeKeys.Count -gt 0) {
            $excludeKeysJson = "[" + (($FKExcludeKeys | ForEach-Object { "`"$_`"" }) -join ", ") + "]"
            $fkArgsList += "exclude_fks: $excludeKeysJson"
        }
        
        if ($fkArgsList.Count -gt 0) {
            $argsString = "{" + ($fkArgsList -join ", ") + "}"
            Write-Host "Executing: dbt run-operation $FKOperation --args '$argsString'" -ForegroundColor Cyan
            & dbt run-operation $FKOperation --args $argsString @DbtArgs
        }
        else {
            Write-Host "Executing: dbt run-operation $FKOperation" -ForegroundColor Cyan
            & dbt run-operation $FKOperation @DbtArgs
        }
        
        $script:DbtExitCode = ($LASTEXITCODE -as [int])
        return
    }

    # -------------------------------
    # Drop mode (selector support)
    # -------------------------------
    if ($DropModels.IsPresent) {
        # 1) Normalize explicit model list (comma/newline/array OK)
        $resolved = @()
        if ($Models) {
            if ($Models.Count -eq 1 -and $Models[0] -match '[,\r\n]') {
                foreach ($m in ($Models[0] -split '[,\r\n]')) {
                    $t = $m.Trim(); if ($t) { $resolved += $t }
                }
            } else {
                foreach ($m in $Models) {
                    $t = $m.Trim(); if ($t) { $resolved += $t }
                }
            }
        }

        # 2) Expand selectors via `dbt ls`
        if ($Selectors) {
            $lsArgs = @('ls','--resource-type','model','--output','name','--select')
            $lsArgs += $Selectors
            if ($Excludes -and $Excludes.Count -gt 0) { $lsArgs += @('--exclude'); $lsArgs += $Excludes }
            
            Write-Host "Resolving selectors via: dbt $($lsArgs -join ' ')"
            $listed = & dbt @lsArgs | Where-Object { $_ -and $_.Trim() -ne '' }
            foreach ($n in $listed) { $resolved += $n.Trim() }
        }

        # 3) De-duplicate AND FORCE array semantics (critical for single-item case)
        $resolved = @($resolved | Sort-Object -Unique)

        if ($resolved.Count -eq 0) {
            Write-Error "No models resolved. Provide -Models and/or -Selectors."
            $script:DbtExitCode = 1
            return
        }

        Write-Host "Resolved models to drop:"
        $resolved | ForEach-Object { Write-Host " - $_" }

        # 4) Safety gate: only drop when confirmed
        if ($ConfirmDrop.IsPresent) {
            $env:ALLOW_TABLE_DROP = '1'
        } else {
            $env:ALLOW_TABLE_DROP = '0'
            Write-Host "ConfirmDrop not set; refusing to drop. (Dry-run above)"
            return
        }

        # 5) Build JSON for --args (models is an ARRAY)
        $argsObj = [ordered]@{
            models = $resolved
            package_name = $PackageName    # empty string is fine (macro ignores)
            confirm = $true
        }
        $argsJson = $argsObj | ConvertTo-Json -Depth 5 -Compress
        Write-Host "Args JSON: $argsJson"

        # 6) Execute drop
        $env:DBT_COMMAND_LINE = "dbt run-operation drop_model_tables --args $argsJson"
        Write-Host "Invoking: $env:DBT_COMMAND_LINE"
        dbt run-operation drop_model_tables --args $argsJson
        $script:DbtExitCode = ($LASTEXITCODE -as [int])

        return
    }

    # -------------------------------
    # Normal passthrough to dbt
    # -------------------------------
    $isRun = $DbtArgs -contains "run"
    & dbt @DbtArgs
    $script:DbtExitCode = ($LASTEXITCODE -as [int])

    if ($script:DbtExitCode -eq 0 -and $isRun) {
        Write-Host "Running show_run_results_html.py..."
        python show_run_results_html.py
        # (optional) don't override dbt's exit code with this script's result
    }
}
catch {
    Write-Error "Error running dbt: $_"
    $script:DbtExitCode = 1
}
finally {
    # Clean up env variables safely
    $vars = @(
        'DBT_COMMAND_LINE',
        'DBT_PYTHON_VERSION',
        'DBT_FABRIC_VERSION',
        'DBT_SQLSERVER_VERSION',
        'DBT_EXECUTION_TIMESTAMP',
        'PYTHONIOENCODING',
        'ALLOW_TABLE_DROP'
    )
    foreach ($v in $vars) {
        try {
            if (Test-Path "Env:$v") {
                Remove-Item -Path "Env:$v" -Force -ErrorAction SilentlyContinue
            }
        } catch { }
    }
}

exit ($script:DbtExitCode -as [int])