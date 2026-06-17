# Rubrik Tiering Existing Snapshots

PowerShell helper for applying or updating Rubrik tiering behavior for existing snapshots.

## File

- `Set-TieringExistingSnapshots.ps1` - operational script for Rubrik snapshot tiering work.

## Requirements

- PowerShell.
- Network access and credentials for the Rubrik environment.
- Any Rubrik PowerShell module or API prerequisites expected by the script.

## Usage

Review the script parameters and environment assumptions first, then run from PowerShell in an approved Rubrik administration context:

```powershell
.\Set-TieringExistingSnapshots.ps1
```

## Caution

This script can affect backup snapshot tiering behavior. Validate against a non-production or limited scope before broad use.
