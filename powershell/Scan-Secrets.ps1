param(
    [string] $RootPath = 'C:\',                    # starting folder (entire disk)
    [string] $LogPath  = 'C:\Temp\SecretScan.csv'  # where to write results
)

# 1. Patterns that *smell* like hard-coded secrets
#    Tune this list for your environment (add specific tokens, keys, etc.).
$Patterns = @(
    'password=',
    'Password=',
    'pwd=',
    'Pwd=',
    'user id=',
    'User ID=',
    'uid=',
    'Uid=',
    'secret=',
    'Secret=',
    'clientsecret',
    'ClientSecret',
    'AccessKey',
    'SharedAccessKey',
    'ConnectionString',
    'connectionstring'
)

# 2. File types to scan as plain text
$textExtensions = @(
    '.txt', '.csv',
    '.ps1', '.psm1', '.psd1',
    '.bat', '.cmd',
    '.py', '.js', '.ts', '.cs', '.java',
    '.xml', '.rdl',
    '.yml', '.yaml',
    '.json', '.config'
    # .docx and .pbix are handled separately as zip-like containers
)

# Optional: exclude noisy system folders
$excludeDirs = @(
    'Windows',
    'Program Files',
    'Program Files (x86)',
    '$Recycle.Bin',
    'ProgramData'
)

# Ensure log directory exists
$logDir = Split-Path -Path $LogPath -Parent
if (-not (Test-Path -LiteralPath $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

# Results container
$results = [System.Collections.Generic.List[object]]::new()

function Add-Result {
    param(
        [string] $Path,
        [string] $Entry,
        [string] $Pattern,
        [int]    $LineNumber,
        [string] $Line
    )

    $results.Add(
        [pscustomobject]@{
            Path       = $Path          # outer file path
            Entry      = $Entry         # inner entry for zip-like files (docx/pbix)
            Pattern    = $Pattern       # pattern that matched
            LineNumber = $LineNumber    # 0 if not applicable
            Line       = $Line          # context line if available
        }
    )
}

Write-Host "Starting secret scan..."
Write-Host "Root path : $RootPath"
Write-Host "Log file  : $LogPath"
Write-Host ""

# 3. Scan "plain" text files
Write-Host "Scanning text files..."
$textFiles = Get-ChildItem -Path $RootPath -Recurse -File -ErrorAction SilentlyContinue |
             Where-Object {
                 $textExtensions -contains $_.Extension -and
                 ($excludeDirs -notcontains $_.Directory.Name)
             }

foreach ($file in $textFiles) {
    try {
        Select-String -Path $file.FullName `
                      -Pattern $Patterns `
                      -SimpleMatch `
                      -AllMatches `
                      -ErrorAction Stop |
        ForEach-Object {
            foreach ($m in $_.Matches) {
                Add-Result -Path       $_.Path `
                           -Entry      '' `
                           -Pattern    $m.Value `
                           -LineNumber $_.LineNumber `
                           -Line       ($_.Line.Trim())
            }
        }
    }
    catch {
        # unreadable or locked files are just skipped
    }
}

# 4. Scan .docx / .pbix as zip-like containers
Write-Host "Scanning .docx and .pbix containers..."

Add-Type -AssemblyName System.IO.Compression.FileSystem -ErrorAction SilentlyContinue

$zipLikeFiles = Get-ChildItem -Path $RootPath -Recurse -File -ErrorAction SilentlyContinue |
                Where-Object {
                    $_.Extension -in @('.docx', '.pbix') -and
                    ($excludeDirs -notcontains $_.Directory.Name)
                }

foreach ($file in $zipLikeFiles) {
    try {
        $fs  = [System.IO.File]::OpenRead($file.FullName)
        $zip = New-Object System.IO.Compression.ZipArchive($fs)

        foreach ($entry in $zip.Entries) {
            # Only bother with XML/text-ish entries inside the archive
            if ($entry.FullName -match '\.(xml|rels|txt|json|config|yml|yaml)$') {

                $stream  = $entry.Open()
                $reader  = New-Object System.IO.StreamReader($stream)
                $content = $reader.ReadToEnd()
                $reader.Close()
                $stream.Close()

                foreach ($pat in $Patterns) {
                    if ($content -like "*$pat*") {
                        # We don't have clean line numbers here; just log the hit
                        Add-Result -Path       $file.FullName `
                                   -Entry      $entry.FullName `
                                   -Pattern    $pat `
                                   -LineNumber 0 `
                                   -Line       ''
                    }
                }
            }
        }

        $zip.Dispose()
        $fs.Close()
        $fs.Dispose()
    }
    catch {
        # corrupted or locked container: ignore
    }
}

# 5. Write results to CSV
$results | Export-Csv -Path $LogPath -NoTypeInformation -Encoding UTF8

Write-Host ""
Write-Host "Scan complete."
Write-Host "Total hits: $($results.Count)"
Write-Host "Log file  : $LogPath"
