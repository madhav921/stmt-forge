# ============================================================
# StmtForge - Scheduled Pipeline Job
# Called by Windows Task Scheduler (CCAnalyser-Pipeline-Daily-9PM)
# ============================================================

$ProjectRoot = "C:\Users\madha\Documents\CCAnalyser"
$Venv        = "$ProjectRoot\.venv\Scripts"
$LogDir      = "$ProjectRoot\data\logs"
$LogFile     = "$LogDir\scheduler_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"

# Force UTF-8 throughout so ₹ and other non-ASCII chars don't crash log capture
$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['Out-File:Encoding'] = 'utf8'

# Ensure log dir exists
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir -Force | Out-Null }

function Write-Log {
    param([string]$Msg)
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $Msg"
    Write-Output $line
    Add-Content -Path $LogFile -Value $line
}

Write-Log "=== StmtForge scheduled run started ==="
Write-Log "Project root : $ProjectRoot"
Write-Log "Log file     : $LogFile"

# Activate venv
$activate = "$Venv\Activate.ps1"
if (-not (Test-Path $activate)) {
    Write-Log "ERROR: Virtual environment not found at $Venv"
    Write-Log "Run: python -m venv .venv && .venv\Scripts\pip install -e . inside $ProjectRoot"
    exit 1
}

. $activate
Write-Log "Virtual environment activated"

# Verify stmtforge is available
$sf = Get-Command stmtforge -ErrorAction SilentlyContinue
if (-not $sf) {
    Write-Log "ERROR: stmtforge command not found in venv. Run: pip install -e . inside $ProjectRoot"
    exit 1
}

# Move to project root so config.yaml is found
Set-Location $ProjectRoot

# Run the pipeline (incremental mode — fetches only new emails/PDFs)
Write-Log "Running: stmtforge run"
& stmtforge run 2>&1 | ForEach-Object { Write-Log $_ }
$exit = $LASTEXITCODE
Write-Log "Pipeline exited with code $exit"

if ($exit -ne 0) {
    Write-Log "WARNING: Pipeline reported a non-zero exit code. Check logs above."
}

Write-Log "=== StmtForge scheduled run finished ==="
exit $exit
