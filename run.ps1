# PowerShell script to install Azure ML packages and run GPU usage monitoring

param(
    [string]$Manager = "jinyli",
    [int]$DaysAgo = 21,
    [string]$ResourceGroup,
    [int]$MaxMlWorkers = 16,
    [int]$MaxBatchWorkers = 16,
    [switch]$ForceInstall,
    [switch]$VerboseLogs
)

# Set error action preference to stop on errors
$ErrorActionPreference = "Stop"

Write-Host "Checking required Python packages..." -ForegroundColor Green

$isCloudShell =
    (-not [string]::IsNullOrWhiteSpace($env:ACC_CLOUD)) -or
    ($env:AZUREPS_HOST_ENVIRONMENT -like "*cloud-shell*") -or
    (-not [string]::IsNullOrWhiteSpace($env:ACC_TERM))

$shouldForceInstall = $ForceInstall -or $isCloudShell

$importCheck = @'
import importlib
modules = [
    "azure.identity",
    "azure.mgmt.resource",
    "azure.mgmt.machinelearningservices",
    "azure.ai.ml",
    "azure.batch",
    "azure.mgmt.batch",
    "azure.mgmt.monitor",
    "msal",
    "requests",
]
def is_missing(module_name):
    try:
        util = getattr(importlib, "util", None)
        if util and hasattr(util, "find_spec"):
            return util.find_spec(module_name) is None
        importlib.import_module(module_name)
        return False
    except Exception:
        return True

missing = [m for m in modules if is_missing(m)]
print(";".join(missing))
'@

if ($shouldForceInstall) {
    if ($ForceInstall) {
        Write-Host "Force install enabled. Installing required packages..." -ForegroundColor Yellow
    }
    elseif ($isCloudShell) {
        Write-Host "Azure Cloud Shell detected. Installing required packages..." -ForegroundColor Yellow
    }
    $missingModules = "ALL"
}
else {
    $missingModules = python -c $importCheck
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to validate Python dependencies. Installing required packages as fallback..." -ForegroundColor Yellow
        $missingModules = "ALL"
    }
}

if ([string]::IsNullOrWhiteSpace($missingModules)) {
    Write-Host "All required packages already installed. Skipping pip install." -ForegroundColor Green
}
else {
    Write-Host "Installing missing packages..." -ForegroundColor Green
    try {
        python -m pip install azure-identity azure-mgmt-resource azure-mgmt-machinelearningservices azure-ai-ml azure-batch azure-mgmt-batch azure-mgmt-monitor
        python -m pip install msal requests
        Write-Host "Packages installed successfully!" -ForegroundColor Green
    }
    catch {
        Write-Host "Failed to install packages: $_" -ForegroundColor Red
        exit 1
    }
}

Write-Host "Checking Azure CLI authentication..." -ForegroundColor Green

# Check if Azure CLI is installed and user is logged in
try {
    $azAccount = az account show --output json 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Azure CLI authentication required. Please run 'az login' first." -ForegroundColor Yellow
        Write-Host "Opening Azure login..." -ForegroundColor Green
        az login
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Azure login failed!" -ForegroundColor Red
            exit 1
        }
    } else {
        $accountInfo = $azAccount | ConvertFrom-Json
        Write-Host "Authenticated as: $($accountInfo.user.name)" -ForegroundColor Green
        Write-Host "Subscription: $($accountInfo.name)" -ForegroundColor Green
    }
}
catch {
    Write-Host "Azure CLI not found. Please install Azure CLI and run 'az login'" -ForegroundColor Red
    Write-Host "Download from: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli" -ForegroundColor Yellow
    exit 1
}

Write-Host "Running GPU usage monitoring script..." -ForegroundColor Green

# Run the Python script
try {
    $pythonArgs = @(
        "./GPU_usage.py",
        "--manager", $Manager,
        "--days-ago", $DaysAgo,
        "--max-ml-workers", $MaxMlWorkers,
        "--max-batch-workers", $MaxBatchWorkers
    )
    if ($ResourceGroup) {
        $pythonArgs += @("--resource-group", $ResourceGroup)
    }
    if ($VerboseLogs) {
        $pythonArgs += "--verbose"
    }

    Write-Host "Command: python $($pythonArgs -join ' ')" -ForegroundColor DarkGray
    python @pythonArgs
    Write-Host "Script completed successfully!" -ForegroundColor Green
}
catch {
    Write-Host "Failed to run GPU_usage.py: $_" -ForegroundColor Red
    exit 1
}