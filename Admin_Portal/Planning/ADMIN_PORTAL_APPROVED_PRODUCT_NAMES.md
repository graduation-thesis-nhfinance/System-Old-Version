# Approved Admin Portal Product Names

Decision date: 2026-09-09  
Scope: Admin Portal display labels only; no Airflow DAG ID or behavior changes.

| Product code | Approved portal display name | Explanation shown in the portal |
| --- | --- | --- |
| `CANDLES` | **Market Price Candles (OHLCV)** | Open, high, low, close, and volume for each market interval. |
| `TRADES` | **Trade Activity** | Completed market trades for the trading pair. |
| `ORDER_BOOK` | **Order Book Depth** | Available buy and sell orders at the collection time. |
| `LIQUIDATIONS` | **Liquidation Events** | Forced position-closure events used for risk analysis. |

These labels replace the preliminary labels in `Plan/ADMIN_PORTAL_TRADING_PAIR_PRODUCT_CATALOG_ADDENDUM.md`. They are presentation/catalog values in the Admin Portal only. Each label maps to its existing protected Airflow DAG ID and must not cause a DAG rename, task rename, or Airflow deployment change.
