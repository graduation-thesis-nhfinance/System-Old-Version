# NHFinance Admin Portal - Ecosystem Command Center

Trung tâm điều phối tập trung dành cho Apache Airflow (SYS-02), ClickHouse (SYS-05), MinIO (SYS-05) và WebSocket Ingestion Service (WSIS).

---

## 1. Tính Năng Nổi Bật

- **Tài khoản quản trị đồng bộ Airflow**: Đăng nhập trực tiếp bằng tài khoản `nhfinanceadmin` (mật khẩu giống với Airflow: `nhathuy123`).
- **Ma trận điều phối 24 Slot (6 Trading Pairs × 4 Data Products)**:
  - 6 Cặp giao dịch: `BTCUSDT`, `ETHUSDT`, `BNBUSDT`, `AAPLBUSDT`, `AMZNBUSDT`, `DELLBUSDT`.
  - 4 Sản phẩm dữ liệu: `Klines (OHLCV)`, `aggTrade (Trade Activity)`, `Order Book Depth`, `Future Liquidation`.
  - Quy tắc khả dụng trung thực (*Honest Availability*): Tự động hiển thị `Product not yet supported` đối với các mã chứng khoán chưa có stream phái sinh thanh lý.
- **Bộ điều phối Khung giờ & Rerun / Reschedule linh hoạt**:
  - Chọn ngày bất kỳ (`YYYY-MM-DD`).
  - Chọn bất kỳ mốc 15 phút nào trong ngày (`00:00`, `00:15`, ..., `23:45`) hoặc chu kỳ 1 giờ.
  - Cho phép **Rerun đích danh 1 ô đơn lẻ** (ví dụ: chỉ chạy lại `DELLBUSDT` + `Klines`).
  - Cho phép **Selective Multi-Rerun** (tích chọn nhiều pairs / nhiều products cùng lúc).
  - Tự động gọi Airflow REST API để kích hoạt DAG run với cấu hình chi tiết hoặc clear task instances.
- **Kết nối 4 Hệ thống**:
  - **Airflow (8085)**: Xem trạng thái các DAG, trigger chạy, mở grid view tab mới.
  - **ClickHouse (8123)**: Thống kê số dòng dữ liệu Gold, kiểm tra độ tươi và QC audits.
  - **MinIO (9000)**: Xem danh sách tệp Bronze/Silver và tạo link tải an toàn (Presigned URL 5 phút).
  - **WSIS**: Giám sát 4 luồng socket 24/7 và trạng thái chốt window 15 phút.
- **Audit Trail**: Nhật ký lưu trữ bất biến mọi thao tác rerun/reschedule của quản trị viên.

---

## 2. Hướng Dẫn Khởi Động Nhanh

### Cách 1: Chạy trực tiếp qua PowerShell
```powershell
.\Admin_Portal\start_admin_portal.ps1
```
Mở trình duyệt truy cập: **`http://localhost:8090`**

### Cách 2: Chạy qua Python Uvicorn
```powershell
cd Admin_Portal/backend
python -m uvicorn app.main:app --host 0.0.0.0 --port 8090 --reload
```

### Cách 3: Chạy qua Docker Compose
```powershell
cd Admin_Portal
docker compose up -d
```

---

## 3. Thông Tin Đăng Nhập
- **Username**: `nhfinanceadmin`
- **Password**: `nhathuy123` (hoặc mật khẩu cấu hình trong `SYS-02/.env`)
