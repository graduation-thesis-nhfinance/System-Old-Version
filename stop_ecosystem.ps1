# ==============================================================================
# 🛑 ENTERPRISE DATA & AI ECOSYSTEM - SHUTDOWN SCRIPT
# ==============================================================================

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Magenta
Write-Host "  🛑 DỪNG VÀ GIẢI PHÓNG TÀI NGUYÊN TOÀN BỘ HỆ SINH THÁI (LOCAL)       " -ForegroundColor Magenta
Write-Host "======================================================================" -ForegroundColor Magenta
Write-Host ""

# Dừng SYS-02 (Airflow)
if (Test-Path "SYS-02/docker-compose.yml") {
    Write-Host "[1/2] Đang dừng SYS-02 (Airflow)..." -ForegroundColor Yellow
    docker-compose -f SYS-02/docker-compose.yml down
    Write-Host "   ✅ SYS-02 đã dừng an toàn." -ForegroundColor Green
}

# Dừng SYS-05 (Storage & Lakehouse)
if (Test-Path "SYS-05/docker-compose.yml") {
    Write-Host "[2/2] Đang dừng SYS-05 (Lakehouse: MinIO, ClickHouse, Postgres)..." -ForegroundColor Yellow
    docker-compose -f SYS-05/docker-compose.yml down
    Write-Host "   ✅ SYS-05 đã dừng an toàn." -ForegroundColor Green
}

# Dừng các compose khác nếu có
$otherServices = @("Admin_Portal", "SYS-01", "SYS-03", "SYS-04")
foreach ($svc in $otherServices) {
    if (Test-Path "$svc/docker-compose.yml") {
        Write-Host "Đang dừng $svc..." -ForegroundColor Yellow
        docker-compose -f "$svc/docker-compose.yml" down
    }
}

Write-Host ""
Write-Host "🎉 Đã dừng toàn bộ hệ sinh thái và giải phóng RAM/CPU!" -ForegroundColor Green
Write-Host ""
