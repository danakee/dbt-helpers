function Update-ModelDataSources {
  param(
    [Parameter(Mandatory)]
    $DatabaseObject,

    [string]$TargetDataSourceName,
    [string]$SQLServerName,
    [string]$SQLDatabaseName,
    [string]$ConnectionStringOverride
  )

  if (-not $DatabaseObject.model -or -not $DatabaseObject.model.dataSources) {
    throw "Model.bim does not contain model.dataSources. Cannot patch connection settings."
  }

  $dataSources = $DatabaseObject.model.dataSources

  $targets = if ($TargetDataSourceName) {
    $dataSources | Where-Object { $_.name -eq $TargetDataSourceName }
  } else {
    $dataSources
  }

  if (-not $targets -or $targets.Count -eq 0) {
    $available = ($dataSources | ForEach-Object { $_.name }) -join ", "
    throw "No matching dataSources found. Requested='$TargetDataSourceName'. Available=[$available]"
  }

  foreach ($ds in $targets) {

    # ------------------------------------------------------------
    # Case 1: classic connectionString
    # ------------------------------------------------------------
    if ($null -ne $ds.PSObject.Properties["connectionString"]) {
      $old = $ds.connectionString

      if ($ConnectionStringOverride) {
        $ds.connectionString = $ConnectionStringOverride
      } else {
        if (-not $SQLServerName -and -not $SQLDatabaseName) {
          throw "Provide -ConnectionStringOverride OR (-SQLServerName and/or -SQLDatabaseName)."
        }
        $ds.connectionString = Update-ConnectionStringParts -ConnectionString $old -NewServer $SQLServerName -NewDatabase $SQLDatabaseName
      }

      Write-Host "Patched datasource '$($ds.name)' via connectionString."
      Write-Host "  OLD: $old"
      Write-Host "  NEW: $($ds.connectionString)"
      continue
    }

    # ------------------------------------------------------------
    # Case 2: connectionDetails.address.server/database
    # Common in newer tabular models
    # ------------------------------------------------------------
    if ($null -ne $ds.PSObject.Properties["connectionDetails"] -and
        $ds.connectionDetails -and
        $ds.connectionDetails.address) {

      $oldServer = $ds.connectionDetails.address.server
      $oldDb     = $ds.connectionDetails.address.database

      if ($ConnectionStringOverride) {
        throw "Datasource '$($ds.name)' uses connectionDetails, not connectionString. Use -SQLServerName/-SQLDatabaseName, not -ConnectionStringOverride."
      }

      if ($SQLServerName)   { $ds.connectionDetails.address.server   = $SQLServerName }
      if ($SQLDatabaseName) { $ds.connectionDetails.address.database = $SQLDatabaseName }

      Write-Host "Patched datasource '$($ds.name)' via connectionDetails.address."
      Write-Host "  OLD server/db: $oldServer / $oldDb"
      Write-Host "  NEW server/db: $($ds.connectionDetails.address.server) / $($ds.connectionDetails.address.database)"
      continue
    }

    # ------------------------------------------------------------
    # Case 3: expression (Power Query / M)
    # Often contains Sql.Database("server","db")
    # ------------------------------------------------------------
    if ($null -ne $ds.PSObject.Properties["expression"] -and $ds.expression) {

      if ($ConnectionStringOverride) {
        throw "Datasource '$($ds.name)' uses expression (M). Provide -SQLServerName/-SQLDatabaseName so we can rewrite Sql.Database(...) calls."
      }

      if (-not $SQLServerName -and -not $SQLDatabaseName) {
        throw "Datasource '$($ds.name)' uses expression (M). Provide -SQLServerName and/or -SQLDatabaseName."
      }

      $oldExpr = $ds.expression

      # Rewrite Sql.Database("server","db") patterns (best-effort)
      $newExpr = $oldExpr

      if ($SQLServerName -and $SQLDatabaseName) {
        $newExpr = [regex]::Replace(
          $newExpr,
          '(?is)Sql\.Database\(\s*"[^"]*"\s*,\s*"[^"]*"\s*\)',
          'Sql.Database("' + $SQLServerName + '","' + $SQLDatabaseName + '")'
        )
      } elseif ($SQLServerName) {
        $newExpr = [regex]::Replace(
          $newExpr,
          '(?is)Sql\.Database\(\s*"[^"]*"\s*,',
          'Sql.Database("' + $SQLServerName + '",'
        )
      } elseif ($SQLDatabaseName) {
        $newExpr = [regex]::Replace(
          $newExpr,
          '(?is)Sql\.Database\(\s*"([^"]*)"\s*,\s*"[^"]*"\s*\)',
          'Sql.Database("$1","' + $SQLDatabaseName + '")'
        )
      }

      if ($newExpr -eq $oldExpr) {
        throw "Datasource '$($ds.name)' has an expression, but no Sql.Database(...) pattern was rewritten. You may need a custom rewrite for your connector."
      }

      $ds.expression = $newExpr

      Write-Host "Patched datasource '$($ds.name)' via expression rewrite."
      Write-Host "  OLD: $oldExpr"
      Write-Host "  NEW: $($ds.expression)"
      continue
    }

    # ------------------------------------------------------------
    # Unknown shape
    # ------------------------------------------------------------
    $props = ($ds.PSObject.Properties.Name -join ", ")
    throw "Datasource '$($ds.name)' has an unsupported structure (properties: $props). Paste its JSON so we can patch it safely."
  }

  return $DatabaseObject
}