$files = Get-ChildItem -Filter *.dtsx

foreach ($f in $files) {
    $path = $f.FullName
    $backup = "$path.bak"

    if (-not (Test-Path $backup)) {
        Copy-Item $path $backup
    }

    $text = Get-Content $path -Raw

    # Only do anything if we see ST170
    if ($text -match 'SSIS_ST170') {
        $text = $text -replace 'Version=17\.0\.0\.0', 'Version=16.0.0.0'
        $text = $text -replace 'SSIS_ST170', 'SSIS_ST160'
        Set-Content $path $text
        Write-Host "Patched $path"
    }
}
