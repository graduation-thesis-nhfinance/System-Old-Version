# 📖 CONTEXT: SYS-05 Lakehouse & Database Management Domain Model

This document defines the **Ubiquitous Language** for **SYS-05 (Lakehouse & Database Management Portal)** within the Financial Analytics & Fraud Detection Enterprise Platform.

---

## 📚 Ubiquitous Language & Terminology

### 1. Storage & Medallion Architecture
- **Medallion Architecture**: Data design pattern organizing storage into 3 progressive quality layers:
  - **Bronze Layer (`crypto-bronze`)**: Raw, immutable, unvalidated payload files (JSON/CSV) received directly from exchange APIs. Retained for 30 days.
  - **Silver Layer (`crypto-silver`)**: Cleaned, standardized, schema-enforced Parquet/Delta Lake files with invalid records filtered out. Retained permanently.
  - **Gold Layer (`crypto-gold`)**: Highly aggregated feature datasets (MA, RSI, VWAP) optimized for machine learning training and analytical consumption. Retained permanently.
- **S3 Object Storage**: S3-compatible object storage (MinIO) managing file-based medallion layers.

### 2. Time-Series Analytical Engine (ClickHouse)
- **Columnar Storage**: Data organization storing table columns contiguously on disk, optimizing analytical aggregation and data compression (5x-10x ratio).
- **ReplacingMergeTree**: ClickHouse table engine that deduplicates rows sharing the same Primary Key based on a version/timestamp column (`updated_at`).
- **SummingMergeTree**: ClickHouse table engine that automatically sums numeric columns sharing the same Primary Key during background merges.
- **Materialized View**: Database object storing pre-aggregated results (`mv_hourly_volume`) updated automatically as new rows enter the source table (`crypto_klines_gold`).
- **Sparse Index (`index_granularity`)**: Index mechanism building an entry every 8,192 rows, providing sub-10ms query speeds without indexing overhead.

### 3. Financial Market Domain Concepts
- **Kline / Candlestick (OHLCV)**: Financial interval data containing Open, High, Low, Close prices, Volume, Quote Asset Volume, and Trade Count.
- **Taker Volume**: Volume initiated by aggressive market orders matching existing limit orders (BUY taker vs SELL taker).
- **Order Flow Imbalance (OFI)**: Metric derived from trade direction (`is_buyer_maker`) measuring aggressive buyer vs seller buying pressure.
- **Order Book Imbalance (OBI)**: Metric derived from top-of-book bid/ask volume distribution measuring supply vs demand pressure.
- **Liquidation Cascade**: Forced closure of leveraged positions (Long or Short) by exchange risk engines during sharp price movements.
- **Trading Pair**: Pair of base and quote assets (e.g. `BTC/USDT`, `AAPLB/USDT`).

### 4. Operational Performance Targets
- **Single-Key Query SLA**: Requirement that single-symbol time-series SQL queries complete in **< 10ms**.
- **Cold-Start Latency**: Initial connection handshake latency when establishing a new TCP/HTTP socket session (~10-30ms).
