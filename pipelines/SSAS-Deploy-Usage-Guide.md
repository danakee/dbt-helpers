# SSAS-Deploy.ps1 — Usage Guide

## Overview

`SSAS-Deploy.ps1` deploys an SSAS 2022 Tabular model from a `Model.bim` file without requiring Visual Studio or Tabular Editor. It supports both legacy and structured (Power Query) datasources, patches connection strings and credentials for the target environment, optionally processes (refreshes) the model after deployment, and can export the generated TMSL/XMLA to disk for audit or dry-run purposes.

The script can run fully interactively (prompting for every parameter with sensible defaults) or fully parameterized for CI/CD pipeline execution.

---

## Prerequisites

- **PowerShell**: Windows PowerShell 5.1 or PowerShell 7+. The script auto-detects the version and adjusts `ConvertFrom-Json` / `ConvertTo-Json` behavior accordingly (the `-Depth` parameter is only available in PS 7+).
- **SqlServer Module**: Required for `Invoke-ASCmd`. The script will auto-install it to `CurrentUser` scope if missing. Not required in export-only mode.
- **SSAS Server Admin**: The account running the script must be an SSAS server-level administrator on the target instance. This is a server-level setting (SSMS → Server Properties → Security), not a database-level role.

---

## Parameters

### Environment Configuration

#### `-Environment`
**Type:** `String` — **Valid values:** `DEV`, `QAT`, `UAT`, `PRD`
**Default (interactive):** `DEV` — **Default (CI):** `DEV`

Controls the target environment. Used to auto-build the default SSAS/SQL server name using the pattern `sql.app.<env>.mydomain.com`. This is also the first parameter checked to determine interactive vs. CI mode — if `-Environment` is passed on the command line, the script assumes non-interactive (CI) mode and will not prompt for any missing parameters, falling back to defaults silently.

#### `-SSASServer`
**Type:** `String`
**Default:** `sql.app.<env>.mydomain.com` (derived from `-Environment`)

The target SSAS instance to deploy to. This is the server that will host the Tabular model database. In most environments, SSAS runs on the same server as the SQL Server relational engine, but this parameter allows targeting a separate SSAS instance if needed.

#### `-SSASDatabase`
**Type:** `String`
**Default:** `SimulationsAnalytics`

The name of the SSAS Tabular database to create or replace. The `name` property inside `Model.bim` is overwritten with this value before deployment, so the .bim file does not need to match the target database name.

---

### Data Source Configuration

#### `-SQLServer`
**Type:** `String`
**Default:** Same as `-SSASServer`

The SQL Server instance that the SSAS datasource should connect to for data during processing. This is patched into the datasource definition — for structured datasources it updates `connectionDetails.address.server` and `credential.path`; for legacy datasources it updates the `connectionString`.

#### `-SQLDatabase`
**Type:** `String`
**Default:** Same as `-SSASDatabase`

The SQL Server database name that the SSAS datasource should connect to. Patched alongside `-SQLServer` into the appropriate datasource properties.

#### `-DataSourceName`
**Type:** `String`
**Default:** `SQL/SimulationsAnalytics`

The name of the datasource within `Model.bim` to patch. Must match the `name` property of a datasource in `model.dataSources[]`. If left blank (empty string), all datasources in the model are patched. When targeting a specific datasource, the script throws an error with a list of available datasource names if no match is found.

#### `-ConnectionStringOverride`
**Type:** `String`
**Default:** Empty (auto-build from `-SQLServer` and `-SQLDatabase`)

Provides a complete connection string to replace the existing one. **Only applicable to legacy datasources** — structured datasources will emit a warning and ignore this parameter. When provided, `-SQLServer` and `-SQLDatabase` are not used for connection string patching (though they may still affect other behavior). In most cases, you should use `-SQLServer` and `-SQLDatabase` instead and let the script build/patch the connection details automatically.

---

### Project / File Paths

#### `-ProjectPath`
**Type:** `String`
**Default:** `C:\source\repos\DIV.Simulations.EDW\SimulationsAnalytics`

The local path to the SSAS Tabular project directory containing the `Model.bim` file. This is typically the repo checkout path on the build agent or your local workstation.

#### `-ModelBimRelativePath`
**Type:** `String`
**Default:** `Model.bim`

The relative path from `-ProjectPath` to the `Model.bim` file. In most cases this is just `Model.bim` at the project root, but if your project structure nests it differently, adjust accordingly.

---

### Deployment Behavior

#### `-Overwrite`
**Type:** `Switch`
**Default (interactive):** Prompted, defaults to `Y` — **Default (CI):** `$false`

Controls whether the script will replace an existing SSAS database. If the target database already exists and `-Overwrite` is not set, the script throws an error. This is a safety mechanism to prevent accidental overwrites in production.

**Important:** `createOrReplace` will wipe database-level role memberships (AD account assignments). If you have a separate script that reapplies role memberships, run it as a subsequent pipeline step after this script completes.

---

### Processing (Refresh)

#### `-Process`
**Type:** `Switch`
**Default (interactive):** Prompted, defaults to `Y` — **Default (CI):** `$false`

When set, the script issues a TMSL `refresh` command after successful deployment. Processing pulls data from the SQL Server datasource into the SSAS model. Without processing, the deployed model will have the correct schema but no data.

**Note on credentials:** The SSAS service account identity is used to connect to SQL Server during processing. The script automatically removes any embedded `Username` from the `Model.bim` credential block so that SSAS falls back to its own service identity. This means the SSAS service account must have appropriate permissions (e.g., `db_datareader`) on the target SQL database.

#### `-ProcessType`
**Type:** `String` — **Valid values:** `full`, `automatic`, `clearValues`
**Default:** `full`

Controls the TMSL refresh type. Only relevant when `-Process` is active.

- **`full`** — Drops all data and reloads from scratch. Longest duration but guarantees a clean state. Use this for initial deployments and when the model schema has changed.
- **`automatic`** — SSAS determines the optimal processing strategy based on what has changed. Faster than full when only data (not schema) has changed.
- **`clearValues`** — Removes all data from the model without reloading. Useful for clearing a cube before a controlled reload, or to reduce server memory usage.

#### `-ProcessTables`
**Type:** `String` (comma-separated table names)
**Default:** Empty (all tables / database-level refresh)

When empty, processing targets the entire database (all tables are refreshed in a single TMSL command). When populated with a comma-separated list of table names, processing issues a table-level refresh targeting only the specified tables. Table names must match exactly as they appear in the model (e.g., `DimDate,FactSimulatorConfiguration`).

**Behavior differences:**
- **Database-level** (no `-ProcessTables`): Single TMSL `refresh` command with a database object. SSAS determines processing order based on dependencies.
- **Table-level** (`-ProcessTables` specified): Single TMSL `refresh` command with multiple table objects. Useful for refreshing only the tables affected by an incremental data load.

---

### XMLA Export

#### `-ExportXmla`
**Type:** `Switch`
**Default (interactive):** Prompted, defaults to `N` — **Default (CI):** `$false`

When set, the script writes the generated TMSL JSON to disk. Two files are produced when both deploy and process are active:

- `createOrReplace_<database>_<timestamp>.xmla.json` — The full deployment TMSL
- `refresh_<database>_<timestamp>.xmla.json` — The refresh/processing TMSL

Files are timestamped with `yyyyMMdd_HHmmss` format. This is useful for auditing what was deployed, debugging deployment failures, or reviewing the TMSL before executing it against a live server.

#### `-ExportOnly`
**Type:** `Switch`
**Default (interactive):** Prompted (only if `-ExportXmla` is `Y`), defaults to `N` — **Default (CI):** `$false`

Dry-run mode. Implies `-ExportXmla`. The script performs all preparation steps (load Model.bim, patch datasource, patch credentials, build TMSL) and writes the XMLA files to disk, but **skips** all server interactions: no SqlServer module install, no SSAS connectivity check, no deployment, no processing.

This is ideal for validating the generated TMSL before running against a real server, or for generating XMLA files to execute manually in SSMS.

#### `-ExportPath`
**Type:** `String`
**Default:** `<ProjectPath>\xmla-export`

The directory where exported XMLA files are written. Created automatically if it does not exist.

---

## Interactive vs. CI Mode

The script determines its mode based on whether `-Environment` was passed on the command line:

**Interactive mode** (`-Environment` not passed): Every parameter that is empty or not provided will prompt the user with a `Read-Host` call showing the default value in brackets. Press Enter to accept the default, or type a new value. This makes it easy to run the script ad-hoc from a terminal with minimal typing.

**CI mode** (`-Environment` passed on the command line): No prompts are issued. Every parameter that is empty falls through to its hardcoded default silently. This ensures the script never hangs waiting for input in an unattended pipeline. Any required parameter that cannot be defaulted will throw an error.

---

## Datasource Types

The script auto-detects the datasource type by checking for the presence of `connectionString` (legacy) or `connectionDetails` (structured) properties on each datasource object.

### Legacy Datasources

Legacy datasources use a flat `connectionString` property (e.g., `Data Source=myserver;Initial Catalog=mydb;...`). The script patches this string using regex replacement of the `Data Source`, `Server`, `Address`, `Initial Catalog`, and `Database` keywords. `-ConnectionStringOverride` can replace the entire string.

### Structured Datasources (Power Query)

Structured datasources use a `connectionDetails` object with nested `address.server` and `address.database` properties, plus a separate `credential` block. The script patches three things:

1. **`connectionDetails.address.server`** and **`address.database`** — Updated to match `-SQLServer` and `-SQLDatabase`.
2. **`credential.Username`** — Removed entirely so SSAS uses its own service identity for data source connections during processing.
3. **`credential.path`** — Rebuilt as `<server>;<database>` to stay in sync with the patched address.
4. **`credential.Password`** — Removed if present (defensive cleanup; passwords should never be in source control).

The remaining credential properties (`AuthenticationKind: Windows`, `kind: SQL`, `EncryptConnection: true`) are preserved as-is.

---

## Pipeline Integration

### Typical Azure DevOps Pipeline Flow

The recommended pipeline sequence for SSAS Tabular deployment is:

1. **Step 1 — Deploy Model** (`SSAS-Deploy.ps1`): Deploys the model definition with patched datasource and credentials. This wipes database-level role memberships.
2. **Step 2 — Apply Role Memberships** (separate script): Reapplies AD account assignments to database roles using scoped `createOrReplace` TMSL commands.
3. **Step 3 — Process** (optional, can be part of Step 1): Refreshes the model data from SQL Server.

### Service Account Requirements

Three accounts are involved in the deployment pipeline:

- **Pipeline agent service account** (gMSA): Runs the ADO build agent and executes the script. Must be an **SSAS server-level administrator** to execute `createOrReplace` and `refresh` commands.
- **SSAS service account**: The identity the SSAS Windows service runs under. This is what connects to SQL Server during processing (after `Username` is stripped from the credential). Must have appropriate SQL Server permissions (e.g., `db_datareader`) on the target database.
- **Developer account**: Your personal account, which may be embedded in `Model.bim` from local development. The script automatically removes this from the credential block during deployment.

---

## Examples

### Fully Interactive (Accept All Defaults)

```powershell
.\SSAS-Deploy.ps1
```

Prompts for every parameter. Press Enter at each prompt to accept the bracketed default.

### Minimal CI Deployment (DEV, No Process)

```powershell
.\SSAS-Deploy.ps1 -Environment DEV -Overwrite
```

Deploys to DEV with all defaults, overwrites existing database, no processing.

### Full CI Deployment with Processing

```powershell
.\SSAS-Deploy.ps1 -Environment QAT -Overwrite -Process -ProcessType full
```

Deploys to QAT, overwrites, and performs a full refresh of all tables.

### Process Specific Tables Only

```powershell
.\SSAS-Deploy.ps1 -Environment UAT -Overwrite -Process -ProcessType full `
  -ProcessTables "DimDate,FactSimulatorConfiguration,FactSimulatorConfigurationComponent"
```

Deploys to UAT and refreshes only the three specified tables.

### Dry-Run Export (No Server Contact)

```powershell
.\SSAS-Deploy.ps1 -Environment PRD -ExportXmla -ExportOnly `
  -ExportPath "C:\temp\ssas-xmla"
```

Generates the TMSL files to `C:\temp\ssas-xmla` without touching any server. Review the exported JSON in a text editor or paste into SSMS to execute manually.

### Deploy + Export for Audit

```powershell
.\SSAS-Deploy.ps1 -Environment PRD -Overwrite -Process -ProcessType full `
  -ExportXmla -ExportPath "\\fileshare\ssas-audit"
```

Deploys and processes against PRD, and saves copies of both the `createOrReplace` and `refresh` TMSL to a network share for audit trail.

### Custom Server Targeting

```powershell
.\SSAS-Deploy.ps1 -Environment DEV `
  -SSASServer "custom-ssas-host.mydomain.com" `
  -SQLServer "custom-sql-host.mydomain.com" `
  -SQLDatabase "SimulationsAnalytics_Staging" `
  -Overwrite -Process -ProcessType full
```

Overrides the auto-generated server names and targets a different SQL database for the datasource connection.

### Fully Parameterized (No Prompts, All Explicit)

```powershell
.\SSAS-Deploy.ps1 `
  -Environment PRD `
  -SSASServer "sql.app.prd.mydomain.com" `
  -SSASDatabase "SimulationsAnalytics" `
  -SQLServer "sql.app.prd.mydomain.com" `
  -SQLDatabase "SimulationsAnalytics" `
  -ProjectPath "C:\source\repos\DIV.Simulations.EDW\SimulationsAnalytics" `
  -ModelBimRelativePath "Model.bim" `
  -DataSourceName "SQL/SimulationsAnalytics" `
  -Overwrite `
  -Process -ProcessType full `
  -ExportXmla -ExportPath "C:\temp\ssas-xmla"
```
