# 🔄 SYS-02: DATA PIPELINE & ORCHESTRATION SYSTEM GUIDE

> **Mã Phân Hệ**: `SYS-02`  
> **Tên Phân Hệ**: Data Pipeline & Orchestration System  
> **Công Nghệ Core**: Apache Airflow 2.8+, Python 3.10, PostgreSQL 15  
> **Port Mặc Định**: Airflow Web UI `8080` | Postgres Host Port `5433` (Đã cách ly cổng với SYS-05)  

---

## 🎯 1. TỔNG QUAN VÀ VAI TRÒ
SYS-02 đóng vai trò là **Bộ não điều phối dữ liệu (Central Data & MLOps Orchestrator)** cho toàn bộ nền tảng Financial Analytics & Fraud Platform.

### Các nhiệm vụ cốt lõi:
1. **Quản lý luồng dữ liệu tài chính (Medallion Ingestion ETL)**: Chạy định kỳ hàng giờ (`@hourly`) nạp dữ liệu Binance, làm sạch Bronze $\rightarrow$ Silver $\rightarrow$ Gold và ghi vào ClickHouse & MinIO S3.
2. **Kích hoạt huấn luyện AI định kỳ (MLOps Retrain Trigger)**: Kích hoạt hàng ngày (`@daily`) để nạp feature mới và gửi request huấn luyện lại các model AI (LSTM, LightGBM, Isolation Forest) trong SYS-04.
3. **Bảo trì dữ liệu định kỳ (Lakehouse Maintenance)**: Dọn dẹp các file Bronze hết hạn (> 30 ngày) trong MinIO và nén nốt các partition ClickHouse hàng tuần (`@weekly`).

---

## 🗂️ 2. DANH MỤC DAGS VÀ CHU KỲ NẠP DỮ LIỆU

| Mã DAG | Tên DAG | Chu Kỳ (Schedule) | Vai trò chính |
| :--- | :--- | :--- | :--- |
| `01_binance_klines_etl_dag` | **Binance Klines Medallion ETL** | `@hourly` (Hàng giờ) | Extract dữ liệu nến Binance $\rightarrow$ Silver Parquet $\rightarrow$ Data Quality Check Gate $\rightarrow$ Load Gold ClickHouse. |
| `02_ml_model_retrain_dag` | **MLOps Model Retrain Trigger** | `@daily` (Hàng ngày lúc 00:00) | Kiểm tra volume data trong Gold $\rightarrow$ Call FastAPI Serving API retrain $\rightarrow$ Update MLflow Registry. |
| `03_lakehouse_maintenance_dag` | **Lakehouse Purge & Maintenance** | `@weekly` (Chủ Nhật) | Xóa file Bronze MinIO > 30 ngày $\rightarrow$ Optimizing ClickHouse table partitions $\rightarrow$ Vacuum Airflow metadata DB. |

---

## ⚙️ 3. CÁCH LÝ CỔNG VÀ NĂNG LỰC VẬN HÀNH

* **Airflow Web UI**: Port `8080` (`http://localhost:8080`)
* **Metadata Database (Postgres)**: Internal Port `5432` $\rightarrow$ **Host Port `5433`**  
  *(Cấu hình port `5433` đảm bảo hoàn toàn không bị xung đột/trùng cổng với PostgreSQL hoặc ClickHouse của SYS-05)*.

---

## 🚀 4. HƯỚNG DẪN KHỞI ĐỘNG HỆ THỐNG

```bash
# 1. Truy cập thư mục SYS-02
cd e:/Graduation_Thesis/SYS-02

# 2. Khởi chạy Airflow Webserver, Scheduler và Postgres DB
docker-compose up -d --build

# 3. Truy cập Airflow UI:
# URL: http://localhost:8080
# User: admin / Pass: admin
```
