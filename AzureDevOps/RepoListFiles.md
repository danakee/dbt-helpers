# Azure DevOps Repository File Finder

A Python console script to query Azure DevOps repositories and list files matching specific patterns.

## Features

- Search single or multiple repositories
- Filter files by patterns (wildcards supported)
- Search specific branches
- Multiple output formats (simple, detailed, CSV)
- Recursive directory traversal
- Search from specific paths within repositories

## Prerequisites

- Python 3.7 or higher
- Azure DevOps account with access to repositories
- Personal Access Token (PAT) with Code (Read) permissions
- PowerShell 5.1 or higher

## Installation

1. Install dependencies:
```powershell
pip install -r requirements.txt
```

Or install directly:
```powershell
pip install azure-devops msrest
```

## Creating a Personal Access Token (PAT)

1. Go to Azure DevOps: `https://dev.azure.com/{your-organization}`
2. Click on User Settings (icon in top right) → Personal Access Tokens
3. Click "New Token"
4. Give it a name and set expiration
5. Under "Scopes", select "Code" → "Read"
6. Click "Create" and copy the token (save it securely!)

## Usage

### Basic Usage

```powershell
# Search all repositories in a project for Python files
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --pat YOUR_PAT_TOKEN `
    --project YourProject `
    --patterns "*.py"
```

### Using Environment Variable for PAT

```powershell
# Set PAT as environment variable (recommended)
$env:AZURE_DEVOPS_PAT = "your_pat_token"

# Then run without --pat argument
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --project YourProject `
    --patterns "*.py"
```

### Search Specific Repository

```powershell
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --project YourProject `
    --repository MyRepo `
    --patterns "*.json" "*.yaml"
```

### Search Specific Branch

```powershell
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --project YourProject `
    --branch develop `
    --patterns "*.cs"
```

### Search from Specific Path

```powershell
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --project YourProject `
    --path /src/services `
    --patterns "*.ts"
```

### Different Output Formats

```powershell
# Simple format (default): Shows repository and path
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --project YourProject `
    --patterns "*.md"

# Detailed format: Shows all metadata
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --project YourProject `
    --patterns "*.md" `
    --output detailed

# CSV format: Easy to import into Excel/tools
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --project YourProject `
    --patterns "*.md" `
    --output csv > results.csv
```

### Multiple File Patterns

```powershell
# Find configuration files
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --project YourProject `
    --patterns "*.json" "*.yaml" "*.yml" "*.config" "*.xml"

# Find all documentation
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --project YourProject `
    --patterns "*.md" "*.txt" "README*" "CHANGELOG*"
```

### List All Files

```powershell
# No patterns = list everything
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --project YourProject
```

## Command Line Arguments

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `--org-url` | Yes | Azure DevOps organization URL | `https://dev.azure.com/yourorg` |
| `--pat` | No* | Personal Access Token | `abc123...` |
| `--project` | Yes | Project name | `MyProject` |
| `--repository` | No | Specific repository name | `MyRepo` |
| `--branch` | No | Branch name (default: main) | `develop` |
| `--patterns` | No | File patterns to match | `*.py *.json` |
| `--output` | No | Output format (default: simple) | `simple`, `detailed`, `csv` |
| `--path` | No | Starting path (default: /) | `/src/components` |

*PAT can be provided via `AZURE_DEVOPS_PAT` environment variable

## Pattern Examples

| Pattern | Matches |
|---------|---------|
| `*.py` | All Python files |
| `*.js` `*.ts` | JavaScript and TypeScript files |
| `test_*.py` | Python test files starting with "test_" |
| `*config*` | Any file containing "config" |
| `Dockerfile*` | Dockerfile and variants |
| `*.json` | All JSON files |

## Output Examples

### Simple Output
```
MyRepo: /src/main.py
MyRepo: /tests/test_main.py
OtherRepo: /app.py
```

### Detailed Output
```
Repository: MyRepo
Path:       /src/main.py
Name:       main.py
Size:       2048 bytes
URL:        https://dev.azure.com/...
--------------------------------------------------------------------------------
```

### CSV Output
```
Repository,Path,Name,Size,URL
MyRepo,/src/main.py,main.py,2048,https://...
```

## Common Use Cases

### Find all configuration files
```powershell
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --project MyProject `
    --patterns "*.config" "*.json" "*.yaml" "*.yml" "appsettings.*"
```

### Find all Docker-related files
```powershell
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --project MyProject `
    --patterns "Dockerfile*" "docker-compose*.yml" ".dockerignore"
```

### Find all test files
```powershell
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --project MyProject `
    --patterns "test_*.py" "*_test.py" "*Test.cs" "*Spec.js"
```

### Export to CSV for analysis
```powershell
python RepoListFiles.py `
    --org-url https://dev.azure.com/yourorg `
    --project MyProject `
    --patterns "*.cs" `
    --output csv > csharp_files.csv
```

## Troubleshooting

### "Personal Access Token required"
- Make sure you provide PAT via `--pat` argument or `AZURE_DEVOPS_PAT` environment variable

### "Repository not found"
- Verify the repository name is correct (case-sensitive)
- Check that your PAT has access to the repository

### "Project not found"
- Verify the project name is correct (case-sensitive)
- Ensure your PAT has access to the project

### "Branch not found"
- Check the branch name (usually `main` or `master`)
- Verify the branch exists in the repository

## Security Best Practices

1. **Never commit PAT tokens to source control**
2. Use environment variables for PAT storage
3. Set appropriate PAT expiration dates
4. Use least-privilege access (Code Read only)
5. Rotate PATs regularly

## License

This script is provided as-is for educational and development purposes.