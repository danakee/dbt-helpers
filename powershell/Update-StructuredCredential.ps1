function Update-StructuredCredential {
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

    # Set AuthenticationKind to ServiceAccount
    if ($cred.PSObject.Properties['AuthenticationKind']) {
        $oldKind = $cred.AuthenticationKind
        $cred.AuthenticationKind = "ServiceAccount"
        Write-Host "  Patched credential.AuthenticationKind:"
        Write-Host "    OLD: $oldKind"
        Write-Host "    NEW: ServiceAccount"
    }
    else {
        $cred | Add-Member -NotePropertyName 'AuthenticationKind' -NotePropertyValue "ServiceAccount" -Force
        Write-Host "  Added credential.AuthenticationKind: ServiceAccount"
    }

    # Ensure EncryptConnection is present
    if (-not $cred.PSObject.Properties['EncryptConnection']) {
        $cred | Add-Member -NotePropertyName 'EncryptConnection' -NotePropertyValue $true -Force
        Write-Host "  Added credential.EncryptConnection: true"
    }

    # Remove all properties not needed for ServiceAccount
    foreach ($propName in @('Username', 'Password', 'path', 'kind')) {
        if ($cred.PSObject.Properties[$propName]) {
            $oldVal = $cred.$propName
            $cred.PSObject.Properties.Remove($propName)
            Write-Host "  Removed credential.$propName (was: $oldVal)"
        }
    }
}