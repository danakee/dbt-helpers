


# ===== Main logic =====
try {
    # Discover existing roles
    $existingRoles = Get-ExistingRoles   # map: RoleName -> @{ ID=...; ModelPermission=... }

    foreach ($roleName in $RoleConfig.Keys) {
        $desired = $RoleConfig[$roleName]

        # Desired members (cleaned)
        $desiredMembers = @()
        if ($desired.ContainsKey('Members') -and $desired.Members) {
            $desiredMembers = @(
                $desired.Members |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
                ForEach-Object { $_.Trim() }
            )
        }

        # Desired permission
        $desiredPerm = "read"
        if ($desired.ContainsKey('Permission') -and -not [string]::IsNullOrWhiteSpace($desired.Permission)) {
            $desiredPerm = $desired.Permission
        }

        # Normalize (maps "Admin" -> "administrator", etc.)
        if (Get-Command Normalize-ModelPermission -ErrorAction SilentlyContinue) {
            $desiredPerm = Normalize-ModelPermission $desiredPerm
        }

        # Build distinct desired list with preferred casing
        $desiredDistinct = @{}
        foreach ($m in $desiredMembers) {
            if ([string]::IsNullOrWhiteSpace($m)) { continue }
            $desiredDistinct[$m.ToLowerInvariant()] = $m.Trim()
        }
        $desiredList = @($desiredDistinct.Values)

        if (-not $existingRoles.ContainsKey($roleName)) {
            # ---- Role does not exist: create ----
            Write-Log "Role '$roleName' does not exist. Creating with permission '$desiredPerm' and members: $($desiredList -join ', ')"
            CreateRole -RoleName $roleName -ModelPermission $desiredPerm -Members $desiredList
            Write-Log "Role '$roleName' created."
            continue
        }

        # ---- Role exists: UPSERT members (union) ----
        # Always rehydrate current member names from DMV (works even if role has zero members)
        $mRows = Invoke-DmvQuery -Query " 
SELECT
    R.Name AS RoleName,
    RM.MemberName
FROM `$SYSTEM.TMSCHEMA_ROLES R
LEFT JOIN `$SYSTEM.TMSCHEMA_ROLE_MEMBERSHIPS RM
    ON RM.RoleID = R.ID
"

        $currentList = @(
            $mRows |
            Where-Object { $_.RoleName -eq $roleName -and -not [string]::IsNullOrWhiteSpace($_.MemberName) } |
            ForEach-Object { $_.MemberName.Trim() }
        )

        # Merge current + desired (desired casing wins)
        $mergedNames = Merge-MemberLists -CurrentMembers $currentList -DesiredMembers $desiredList
        $mergedNames = @($mergedNames)  # force array

        # Determine if we need to call alter: only if we are ADDING something
        $currentLower = @{}
        foreach ($m in $currentList) { $currentLower[$m.ToLowerInvariant()] = $true }

        $needChange = $false
        foreach ($m in $desiredList) {
            if (-not $currentLower.ContainsKey($m.ToLowerInvariant())) {
                $needChange = $true
                break
            }
        }

        # (Optional) Helpful debug
        Write-Log "DEBUG '$roleName' current=[$($currentList -join ', ')] desired=[$($desiredList -join ', ')] merged=[$($mergedNames -join ', ')]"

        if ($needChange) {
            Write-Log "Role '$roleName' missing desired members; applying UPSERT (union). Result: $($mergedNames -join ', ')"
            AlterRoleMembers -RoleName $roleName -Members $mergedNames
            Write-Log "Role '$roleName' members updated (UPSERT)."
        } else {
            Write-Log "Role '$roleName' already contains all desired members. No change."
        }
    }

    # Verification (optional): summarize final memberships
    $finalRows = Invoke-DmvQuery -Query "
SELECT
    R.Name AS RoleName,
    RM.MemberName
FROM `$SYSTEM.TMSCHEMA_ROLES R
LEFT JOIN `$SYSTEM.TMSCHEMA_ROLE_MEMBERSHIPS RM
    ON RM.RoleID = R.ID
"
    foreach ($roleName in $RoleConfig.Keys) {
        $finalList = @(
            $finalRows |
            Where-Object { $_.RoleName -eq $roleName -and -not [string]::IsNullOrWhiteSpace($_.MemberName) } |
            ForEach-Object { $_.MemberName }
        )
        Write-Log "Final members for role '$roleName': $($finalList -join ', ')"
    }

    Write-Log "Completed SSAS role management for database '$DatabaseName'."
}
catch {
    Write-Log "Unhandled error: $($_.Exception.Message)" "ERROR"
    throw
}