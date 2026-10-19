# ==============================================================================
# 🚀 ENTERPRISE DATA & AI ECOSYSTEM - STARTUP ORCHESTRATOR
# ==============================================================================

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  🌟 KHỞI ĐỘNG HỆ SINH THÁI DOANH NGHIỆP MINI (ALL-IN-ONE LOCAL)     " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

# 1. Kiểm tra Docker Daemon
Write-Host "[1/4] Kiểm tra trạng thái Docker Desktop..." -ForegroundColor Yellow
try {
    $dockerCheck = docker info 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ LỖI: Docker Desktop chưa được bật!" -ForegroundColor Red
        Write-Host "👉 Vui lòng mở ứng dụng Docker Desktop trên Windows và đợi Docker báo 'Engine Running', sau đó chạy lại script này." -ForegroundColor Magenta
        exit 1
    }
    Write-Host "   ✅ Docker Engine đang chạy bình thường." -ForegroundColor Green
} catch {
    Write-Host "❌ LỖI: Không tìm thấy lệnh docker trong PATH!" -ForegroundColor Red
    exit 1
}

# 2. Khởi động SYS-05: Storage & Lakehouse (MinIO, ClickHouse, Postgres)
Write-Host ""
Write-Host "[2/4] Đang khởi động SYS-05: Lakehouse & Database Management Portal..." -ForegroundColor Yellow
if (Test-Path "SYS-05/docker-compose.yml") {
    docker-compose -f SYS-05/docker-compose.yml up -d
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   ✅ SYS-05 Containers (MinIO, ClickHouse, Postgres) đã bật thành công." -ForegroundColor Green
    } else {
        Write-Host "   ⚠️ Cảnh báo: SYS-05 gặp sự cố khi khởi động." -ForegroundColor Red
    }
} else {
    Write-Host "   ⚠️ Không tìm thấy file SYS-05/docker-compose.yml" -ForegroundColor DarkGray
}

# 3. Đợi vài giây cho Database và MinIO sẵn sàng rồi khởi động SYS-02: Airflow
Write-Host ""
Write-Host "[3/4] Đang khởi động SYS-02: Data Pipeline & Orchestration (Airflow)..." -ForegroundColor Yellow
Start-Sleep -Seconds 3
if (Test-Path "SYS-02/docker-compose.yml") {
    docker-compose -f SYS-02/docker-compose.yml up -d
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   ✅ SYS-02 Airflow (Webserver, Scheduler, Metadata DB) đã bật thành công." -ForegroundColor Green
    } else {
        Write-Host "   ⚠️ Cảnh báo: SYS-02 gặp sự cố khi khởi động." -ForegroundColor Red
    }
} else {
    Write-Host "   ⚠️ Không tìm thấy file SYS-02/docker-compose.yml" -ForegroundColor DarkGray
}

# 4. Kiểm tra các dịch vụ khác (Admin_Portal, SYS-04, WSIS, SYS-06, SYS-01, SYS-03) nếu có file compose
$otherServices = @("Admin_Portal", "SYS-04", "WSIS", "SYS-06", "SYS-01", "SYS-03")
foreach ($svc in $otherServices) {
    if (Test-Path "$svc/docker-compose.yml") {
        Write-Host ""
        Write-Host "Đang khởi động $svc..." -ForegroundColor Yellow
        docker-compose -f "$svc/docker-compose.yml" up -d
    }
}

# 5. Hoàn tất & Mở Cổng điều hướng tập trung (Portal)
Write-Host ""
Write-Host "[4/4] Hoàn tất! Đang mở Cổng điều hướng Unified Ecosystem Portal..." -ForegroundColor Green
Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  🎉 HỆ THỐNG ĐÃ SẴN SÀNG ĐỂ KIỂM TRA & DEMO                         " -ForegroundColor Green
Write-Host "  👉 Portal:             file://$((Get-Location).Path.Replace('\','/'))/index.html" -ForegroundColor White
Write-Host "  👉 Admin Portal:       http://localhost:8090 (nhfinanceadmin / nhathuy123)" -ForegroundColor White
Write-Host "  👉 SYS-04 AI Models:   http://localhost:8000/docs" -ForegroundColor White
Write-Host "  👉 SYS-02 Airflow UI:  http://localhost:8085 (admin123 / admin123)" -ForegroundColor White
Write-Host "  👉 SYS-05 MinIO UI:    http://localhost:9001 (admin123 / admin123)" -ForegroundColor White
Write-Host "  👉 SYS-05 ClickHouse:  http://localhost:8123/play" -ForegroundColor White
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

# Mở portal trên trình duyệt mặc định
Start-Process "index.html"
