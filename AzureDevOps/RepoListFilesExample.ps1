# Example PowerShell script to run RepoListFiles.py
# Demonstrates how to set environment variables and run the script

# CONFIGURATION - Update these values for your environment
$OrgUrl = "https://dev.azure.com/yourorg"
$ProjectName = "YourProject"
$RepositoryName = "YourRepo"  # Optional - leave empty to search all repos
$BranchName = "main"
$FilePatterns = @("*.py", "*.json", "*.cs")  # Files to search for

# Set your Personal Access Token as environment variable
# IMPORTANT: Get your PAT from Azure DevOps User Settings > Personal Access Tokens
$env:AZURE_DEVOPS_PAT = "your_pat_token_here"

# Verify Python is installed
try {
    $pythonVersion = python --version
    Write-Host "Using $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "Error: Python is not installed or not in PATH" -ForegroundColor Red
    exit 1
}

# Build the command arguments
$arguments = @(
    "RepoListFiles.py"
    "--org-url", $OrgUrl
    "--project", $ProjectName
    "--branch", $BranchName
    "--patterns"
) + $FilePatterns

# Add repository if specified
if ($RepositoryName) {
    $arguments = @("RepoListFiles.py", "--org-url", $OrgUrl, "--project", $ProjectName, "--repository", $RepositoryName, "--branch", $BranchName, "--patterns") + $FilePatterns
}

Write-Host "`nSearching Azure DevOps repositories..." -ForegroundColor Cyan
Write-Host "Organization: $OrgUrl" -ForegroundColor Yellow
Write-Host "Project: $ProjectName" -ForegroundColor Yellow
Write-Host "Patterns: $($FilePatterns -join ', ')" -ForegroundColor Yellow
Write-Host ""

# Run the Python script
& python @arguments

# Check exit code
if ($LASTEXITCODE -eq 0) {
    Write-Host "`nSearch completed successfully!" -ForegroundColor Green
} else {
    Write-Host "`nSearch failed with exit code: $LASTEXITCODE" -ForegroundColor Red
}