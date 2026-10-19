# Confirmed Admin Portal Scope: Six Trading Pairs

Decision date: 2026-09-10  
Applies to: Admin Portal prototype and teammate implementation

## Confirmed policy

The Admin Portal shows all six configured market symbols and all four portal products for each symbol: **24 product slots** in total.

| # | Configured symbol |
| --- | --- |
| 1 | `BTCUSDT` |
| 2 | `ETHUSDT` |
| 3 | `BNBUSDT` |
| 4 | `AAPLBUSDT` |
| 5 | `AMZNBUSDT` |
| 6 | `DELLBUSDT` |

Portal products per symbol:

1. Market Price Candles (OHLCV)
2. Trade Activity
3. Order Book Depth
4. Liquidation Events

## Honest availability rule

Configuration is not proof that a source is active or that a particular product is currently supported. The Admin Portal must show an explicit state for every slot:

- `Collected once`, `Queued`, `Running`, `Done`, or `Failed` when an execution exists.
- `Unavailable from source` when the configured symbol cannot be collected from the source.
- `Product not yet supported` when the current Airflow implementation does not collect that product for that symbol.

Do not hide a configured symbol and do not show it as successful when the source or current DAG does not support it.

## Current Airflow limitation to expose

SYS-02 DAG `04_binance_market_metrics_15m_dag` configures all six symbols. Its current Liquidation Events collector streams only the first three configured symbols. Therefore the portal must render Liquidation Events for the other three symbols as `Product not yet supported` until the DAG implementation is expanded.

## Prototype acceptance criteria

- Six pair cards are visible in the Trading-Pair Explorer.
- Each card lists all four approved product names.
- The schedule page represents 24 product slots.
- Unsupported/unavailable slots use an explicit neutral or warning status with an explanation.
- Search filters include every configured symbol.
