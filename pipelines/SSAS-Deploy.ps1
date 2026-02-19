<#
.SYNOPSIS
    SSAS Tabular Deployment Script (Shell)

.DESCRIPTION
    Called from Azure DevOps pipeline.
    Currently validates parameters and logs execution context only.

.PARAMETER Environment
    Target environment name (DEV, QAT, UAT, PRD)

.PARAMETER Server
    Target SSAS server FQDN

.PARAMETER ProjectPath
    Path to the SSAS solution/project folder
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("DEV","QAT","UAT","PRD")]
    [string]$Environment,

    [Parameter(Mandatory = $true)]
    [string]$Server,

    [Parameter(Mandatory = $true)]
    [string]$ProjectPath
)

$ErrorActionPreference = "Stop"

try {

    Write-Host "================================================="
    Write-Host "SSAS DEPLOY SCRIPT (SHELL)"
    Write-Host "================================================="
    Write-Host "Timestamp        : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    Write-Host "Environment      : $Environment"
    Write-Host "Target Server    : $Server"
    Write-Host "Project Path     : $ProjectPath"
    Write-Host "-------------------------------------------------"
    Write-Host "Agent User       : $([System.Security.Principal.WindowsIdentity]::GetCurrent().Name)"
    Write-Host "Agent Machine    : $env:COMPUTERNAME"
    Write-Host "PowerShell Ver   : $($PSVersionTable.PSVersion)"
    Write-Host "Working Directory: $PWD"
    Write-Host "================================================="

    # --- Basic validation ---
    if (-not (Test-Path -LiteralPath $ProjectPath)) {
        throw "ProjectPath not found: $ProjectPath"
    }

    Write-Host ""
    Write-Host "Project folder exists. (Deployment logic will go here later.)"
    Write-Host ""

    Write-Host "SSAS-Deploy.ps1 completed successfully."
    exit 0
}
catch {
    Write-Error "SSAS deployment failed: $_"
    exit 1
}
