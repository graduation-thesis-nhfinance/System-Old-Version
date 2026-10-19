# ==============================================================================
# NHFINANCE ADMIN PORTAL - ECOSYSTEM ORCHESTRATOR LAUNCHER
# ==============================================================================
Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  KHOI DONG ADMIN PORTAL (PORT 8090)                                  " -ForegroundColor Cyan
Write-Host "  Airflow + ClickHouse + MinIO + WSIS Command Center                  " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

$PSScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition
$BackendPath = Join-Path $PSScriptRoot "backend"

$env:PYTHONPATH = $BackendPath

Write-Host "-> Backend Directory: $BackendPath" -ForegroundColor Gray
Write-Host "-> Starting FastAPI server at http://127.0.0.1:8090 ..." -ForegroundColor Green
Write-Host "-> Login Credentials: Username = nhfinanceadmin | Password = (Airflow Password)" -ForegroundColor Yellow
Write-Host ""

python -m uvicorn app.main:app --host 0.0.0.0 --port 8090 --app-dir "$BackendPath" --reload
