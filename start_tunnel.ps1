# ==============================================================================
# PUBLIC DEMO TUNNEL - CLOUDFLARE / LOCALTUNNEL / NGROK
# ==============================================================================

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  KHOI DONG DUONG HAM DEMO CONG KHAI (PUBLIC HTTPS TUNNEL 0D)         " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Chon dich vu ban muon public duong ham HTTPS ra ngoai Internet:" -ForegroundColor Yellow
Write-Host "  [1] NHFinance Admin Portal (Port 8090) [KHUYEN DUNG - ALL-IN-ONE]" -ForegroundColor Green
Write-Host "  [2] SYS-02: Apache Airflow (Port 8085)" -ForegroundColor White
Write-Host "  [3] SYS-05: MinIO S3 Console (Port 9001)" -ForegroundColor White
Write-Host "  [4] SYS-01: Streamlit Dashboard (Port 8501)" -ForegroundColor White
Write-Host "  [5] SYS-04: FastAPI Swagger Docs (Port 8000)" -ForegroundColor White
Write-Host "  [6] SYS-03: Grafana Monitoring (Port 3000)" -ForegroundColor White
Write-Host "  [7] Tuy chon Port khac" -ForegroundColor White
Write-Host ""

$choice = Read-Host "Nhap lua chon cua ban (1-7) [Mac dinh: 1 - Admin Portal]"
if ([string]::IsNullOrWhiteSpace($choice)) { $choice = "1" }

$targetPort = 8090
$serviceName = "NHFinance Admin Portal"

switch ($choice) {
    "1" { $targetPort = 8090; $serviceName = "NHFinance Admin Portal" }
    "2" { $targetPort = 8085; $serviceName = "SYS-02 Airflow" }
    "3" { $targetPort = 9001; $serviceName = "SYS-05 MinIO Console" }
    "4" { $targetPort = 8501; $serviceName = "SYS-01 Streamlit Dashboard" }
    "5" { $targetPort = 8000; $serviceName = "SYS-04 FastAPI" }
    "6" { $targetPort = 3000; $serviceName = "SYS-03 Grafana" }
    "7" { 
        $custom = Read-Host "Nhap so cong Port can mo"
        $targetPort = [int]$custom
        $serviceName = "Custom Port $targetPort"
    }
    Default { $targetPort = 8090; $serviceName = "NHFinance Admin Portal" }
}

Write-Host ""
Write-Host "Dang chuan bi duong ham HTTPS cho $serviceName (Port $targetPort)..." -ForegroundColor Green

# 1. Thu dung Cloudflare Tunnel (Mien phi 100%, khong can dang ky tai khoan)
$hasCloudflared = Get-Command cloudflared -ErrorAction SilentlyContinue
if ($hasCloudflared) {
    Write-Host "Su dung Cloudflare Tunnel (Mien phi & Bao mat)..." -ForegroundColor Cyan
    Write-Host "Duong link public https://...trycloudflare.com se xuat hien ben duoi:" -ForegroundColor Yellow
    cloudflared tunnel --url "http://localhost:$targetPort"
    exit 0
}

# 2. Thu dung npx localtunnel (Node.js)
$hasNpx = Get-Command npx -ErrorAction SilentlyContinue
if ($hasNpx) {
    Write-Host "Su dung Localtunnel qua NPX (Node.js)..." -ForegroundColor Cyan
    Write-Host "Duong link public https://...loca.lt se xuat hien ben duoi:" -ForegroundColor Yellow
    npx localtunnel --port $targetPort
    exit 0
}

# 3. Thu dung Ngrok
$hasNgrok = Get-Command ngrok -ErrorAction SilentlyContinue
if ($hasNgrok) {
    Write-Host "Su dung Ngrok..." -ForegroundColor Cyan
    ngrok http $targetPort
    exit 0
}

# Neu chua cai cong cu nao
Write-Host "Chua phat hien cong cu Tunnel (cloudflared, localtunnel hoac ngrok) trong may." -ForegroundColor Yellow
Write-Host ""
Write-Host "HUONG DAN CAI DAT NHANH 1 TRONG CAC CONG CU SAU (CHI CAN 1 CONG CU):" -ForegroundColor Magenta
Write-Host "  Cach 1 (Khuyen dung): Tai cloudflared.exe tu https://github.com/cloudflare/cloudflared/releases" -ForegroundColor White
Write-Host "                        hoac chay lenh: winget install Cloudflare.cloudflared" -ForegroundColor White
Write-Host "  Cach 2: Cai Node.js roi chay lai script nay (se tu dong dung npx localtunnel)" -ForegroundColor White
Write-Host "  Cach 3: Tai ngrok tu https://ngrok.com/download" -ForegroundColor White
Write-Host ""
