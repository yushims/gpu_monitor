# GPU Monitor

Monitor recent Azure ML and Azure Batch GPU jobs for a manager’s org, grouped by user and cluster.

## What it does

- Authenticates with Azure using `DefaultAzureCredential`.
- Resolves a manager and direct reports from Microsoft Graph.
- Scans supported Azure resource groups across configured subscriptions.
- Aggregates job counts and GPU usage by:
  - creator (manager org member vs Others)
  - status (`Running`, `Queued`)
  - cluster type (`V100`, `A100`, `H200`, `V100 Batch`)

## Files

- `GPU_usage.py`: main scanner and summary output.
- `gpu_helpers.py`: helper functions (resource resolution, bucketing, normalization).
- `run.ps1`: PowerShell wrapper for dependency check/install, Azure auth check, and script execution.

## Requirements

- Python 3.8+
- Azure CLI installed (`az`)
- Azure login (`az login`)
- Access to:
  - Azure Resource Manager / ML / Batch resources
  - Microsoft Graph user + direct reports lookup for the selected manager

## Python dependencies

`run.ps1` installs/checks these packages:

- `azure-identity`
- `azure-mgmt-resource`
- `azure-mgmt-machinelearningservices`
- `azure-ai-ml`
- `azure-batch`
- `azure-mgmt-batch`
- `azure-mgmt-monitor`
- `msal`
- `requests`

## Quick start (PowerShell wrapper)

Run with defaults:

```powershell
./run.ps1
```

Run with custom manager/time window/resource group:

```powershell
./run.ps1 -Manager rujiang -DaysAgo 1 -ResourceGroup speech-sing-am
```

Enable verbose app logs:

```powershell
./run.ps1 -VerboseLogs
```

Force reinstall dependencies (useful for ephemeral environments):

```powershell
./run.ps1 -ForceInstall
```

### Cloud Shell behavior

`run.ps1` auto-detects Azure Cloud Shell and automatically installs dependencies each run. You can still use `-ForceInstall` explicitly.

## Direct Python usage

Show CLI options:

```powershell
python ./GPU_usage.py --help
```

Example:

```powershell
python ./GPU_usage.py --manager rujiang --days-ago 1 --resource-group speech-sing-am --max-ml-workers 16 --max-batch-workers 16 --verbose
```

## CLI arguments (`GPU_usage.py`)

- `--manager` (default: `jinyli`)
- `--days-ago` (default: `21`)
- `--resource-group` (default: none)
- `--max-ml-workers` (default: `16`)
- `--max-batch-workers` (default: `16`)
- `--verbose` (flag)

Notes:
- Manager aliases without domain are normalized to `@microsoft.com`.
- Resource group matching is case-insensitive.

## Performance notes

- Worker caps are tunable via both `run.ps1` and `GPU_usage.py`.
- Current defaults are `16/16` (`ML/Batch`) as a good balance from local benchmarks.
- Runtime variance is expected due to remote API latency.

## Expected output

On a successful run, you should see:

- Package check/install status
- Azure authentication details
- Subscription + resource group scan logs
- Final per-user and per-cluster summary
- `Script completed successfully!`

Example (trimmed):

```text
Checking required Python packages...
All required packages already installed. Skipping pip install.
Checking Azure CLI authentication...
Authenticated as: <your-alias>@microsoft.com
Running GPU usage monitoring script...
INFO:gpu_monitor:Full-time employees
INFO:gpu_monitor:  Rui Jiang (rujiang)
INFO:gpu_monitor:Subscription: AI Platform GPU 21 - Cognitive Services
INFO:gpu_monitor:  Resource Group: speech-sing-am
...
INFO:gpu_monitor:Summary of jobs submitted in the last 1 days (...):
INFO:gpu_monitor:  Others: 5 Running jobs with 328 GPUs
INFO:gpu_monitor:  Others: 1 Queued jobs with 8 GPUs
INFO:gpu_monitor:  Rui Jiang: 1 Running jobs with 16 GPUs
INFO:gpu_monitor:  Cluster: V100
INFO:gpu_monitor:    rujiang's FTE:
INFO:gpu_monitor:      1 Running jobs with 16 GPUs
INFO:gpu_monitor:    Others:
INFO:gpu_monitor:      5 Running jobs with 328 GPUs
INFO:gpu_monitor:      1 Queued jobs with 8 GPUs
Script completed successfully!
```

## Troubleshooting

- If dependency checks fail in unusual Python environments, `run.ps1` falls back to install mode.
- If Azure auth fails, run `az login` and retry.
- If Graph lookup fails, verify the manager alias/UPN and Graph access permissions.
