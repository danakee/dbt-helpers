$files = Get-ChildItem -Filter *.dtsx -Recurse

foreach ($f in $files) {
    $path   = $f.FullName
    $backup = "$path.bak"

    # One-time backup
    if (-not (Test-Path $backup)) {
        Copy-Item $path $backup
    }

    $text = Get-Content $path -Raw

    # Only touch files that clearly have v17 markers
    if ($text -match 'SSIS_ST170' -or $text -match 'Version=17\.0\.0\.0' -or $text -match 'VSTAMajorVersion="17"' -or $text -match 'VSTAMajorVerion="17"') {

        # Script Task host & assembly versions
        $text = $text -replace 'SSIS_ST170', 'SSIS_ST160'
        $text = $text -replace 'Version=17\.0\.0\.0', 'Version=16.0.0.0'

        # VSTA major version (correct spelling)
        $text = $text -replace 'VSTAMajorVersion="17"', 'VSTAMajorVersion="16"'
        # Just in case of typo in XML
        $text = $text -replace 'VSTAMajorVerion="17"', 'VSTAMajorVerion="16"'

        # Write back as UTF8 to preserve encoding
        Set-Content -Path $path -Value $text -Encoding UTF8

        Write-Host "Patched $path"
    }
}
