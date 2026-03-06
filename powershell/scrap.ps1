function Update-StructuredCredential {
    <#
    .SYNOPSIS
        Patches the credential block on a structured datasource:
        - Sets impersonationMode to impersonateServiceAccount
        - Removes explicit Username/Password (service account handles auth)
        - Rebuilds path to match patched server/database
        - Preserves AuthenticationKind, kind, EncryptConnection, etc.
    #>
    param(
        [Parameter(Mandatory)]$DataSourceObject,
        [string]$PatchedServer,
        [string]$PatchedDatabase
    )

    # ── Set impersonationMode at the datasource level ──
    if ($DataSourceObject.PSObject.Properties['impersonationMode']) {
        $oldMode = $DataSourceObject.impersonationMode
        $DataSourceObject.impersonationMode = "impersonateServiceAccount"
        Write-Host "  Patched impersonationMode:"
        Write-Host "    OLD: $oldMode"
        Write-Host "    NEW: $($DataSourceObject.impersonationMode)"
    }
    else {
        $DataSourceObject | Add-Member -NotePropertyName 'impersonationMode' -NotePropertyValue "impersonateServiceAccount" -Force
        Write-Host "  Added impersonationMode: impersonateServiceAccount"
    }

    if (-not $DataSourceObject.PSObject.Properties['credential']) {
        Write-Host "  No credential block found — nothing to patch."
        return
    }

    $cred = $DataSourceObject.credential

    # ── Remove explicit Username (service account handles auth) ──
    if ($cred.PSObject.Properties['Username']) {
        $oldUser = $cred.Username
        $cred.PSObject.Properties.Remove('Username')
        Write-Host "  Removed credential.Username (was: $oldUser)"
    }

    # ── Remove explicit Password ──
    if ($cred.PSObject.Properties['Password']) {
        $cred.PSObject.Properties.Remove('Password')
        Write-Host "  Removed credential.Password"
    }

    # ── Rebuild path to match patched server;database ──
    if ($cred.PSObject.Properties['path']) {
        $oldPath = $cred.path

        $pathServer   = $PatchedServer
        $pathDatabase = $PatchedDatabase

        # If either wasn't patched, try to preserve the original from the path
        if (-not $pathServer -or -not $pathDatabase) {
            $pathParts = $oldPath -split ';', 2
            if (-not $pathServer   -and $pathParts.Count -ge 1) { $pathServer   = $pathParts[0] }
            if (-not $pathDatabase -and $pathParts.Count -ge 2) { $pathDatabase = $pathParts[1] }
        }

        $newPath = "$pathServer;$pathDatabase"
        $cred.path = $newPath
        Write-Host "  Patched credential.path:"
        Write-Host "    OLD: $oldPath"
        Write-Host "    NEW: $newPath"
    }
}



Update-StructuredCredential `
                -DataSourceObject $ds `
                -PatchedServer $finalServer `
                -PatchedDatabase $finalDatabase



function Update-ModelDataSources {
    param(
        [Parameter(Mandatory)]$DatabaseObject,
        [string]$TargetDataSourceName,
        [string]$SQLServer,
        [string]$SQLDatabase,
        [string]$ConnectionStringOverride
    )



$databaseObj = Update-ModelDataSources `
        -DatabaseObject $databaseObj `
        -TargetDataSourceName $DataSourceName `
        -SQLServer $SQLServer `
        -SQLDatabase $SQLDatabase `
        -ConnectionStringOverride $ConnectionStringOverride


