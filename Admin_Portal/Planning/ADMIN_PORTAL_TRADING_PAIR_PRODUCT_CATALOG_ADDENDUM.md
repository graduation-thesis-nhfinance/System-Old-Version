# Admin Portal Addendum: Trading-Pair Product Catalog

Related specifications:

- `Plan/ADMIN_PORTAL_BUILD_AND_DEPLOYMENT_SPEC.md`
- `Plan/ADMIN_PORTAL_COMPANY_REFERENCE_ADDENDUM.md`

## Decision

Each trading pair must display **four collection products** in the Admin Portal. The portal uses clear business-facing product names, while preserving the existing Airflow DAG IDs and task names unchanged.

The Admin Portal is a presentation and control layer. It maps a portal product to an Airflow DAG; it does not rename, edit, or otherwise change the DAG in Airflow.

## Initial product labels

Use these default labels unless the team confirms different DAG purposes:

| Portal product code | Portal name | Plain-language description | Typical source data |
| --- | --- | --- | --- |
| `CANDLES` | Market Candles (OHLCV) | Open, high, low, close, and trading volume over an interval. | Klines/candles DAG |
| `TRADES` | Trade Activity | Individual or aggregated executed trades. | Aggregate-trades DAG |
| `ORDER_BOOK` | Order Book Snapshots | Bid/ask depth captured at defined times. | Depth-snapshot DAG |
| `LIQUIDATIONS` | Liquidation Events | Forced liquidation events used for risk/anomaly analysis. | Liquidations DAG |

The labels may be changed in the Admin Portal catalog—for example, to Vietnamese or company terminology—without any Airflow deployment. Portal users must not edit the stored Airflow DAG ID through the normal schedule screen.

## Required mapping model

Create a portal-owned catalog row for each product/DAG mapping:

| Field | Requirement |
| --- | --- |
| `product_id` | Stable portal UUID. |
| `product_code` | Stable readable code, e.g. `CANDLES`. |
| `display_name` | Editable portal label shown in UI. |
| `description` | Editable business explanation. |
| `airflow_dag_id` | Immutable mapping key after approval; used only by the trigger adapter. |
| `default_schedule_id` | Optional default schedule template. |
| `supports_trading_pair` | `true` for all four initial products. |
| `quality_profile` | Required quality policy/version for this product. |
| `active` | Controls portal availability; must not pause/rename Airflow directly. |
| `catalog_version`, `updated_by`, `updated_at` | Audit fields. |

Only an Administrator may create or change a mapping. Changing `airflow_dag_id` requires a confirmation flow, an audit reason, and successful adapter validation. A normal product-name change affects only `display_name` and `description`.

## Schedule and daily-key behavior

For every trading pair, generate or configure up to four schedules—one for each active product. The one-collection-per-day key becomes:

```text
(schedule_id, trading_pair, product_id, input_day)
```

This means BTCUSDT may have one normal daily Candles run, one Trade Activity run, one Order Book Snapshot run, and one Liquidation Events run for the same input day. They are separate products and do not block one another. A second Candles collection for the same trading pair and input day is blocked by the daily policy unless it is an approved recollection.

## UI requirements

### Trading-pair page

Provide a pair detail page, for example `BTCUSDT`, with four product cards. Each card shows:

- portal product name and description;
- active/inactive state;
- schedule and timezone;
- last input day and normal/recollection attempt;
- Collection, Data Quality, Transform, Gold, and overall status;
- authoritative quality-approved batch ID;
- next scheduled run; and
- `View runs` plus a policy-gated `Run now` action.

The page must state “4 configured products” and show missing/inactive products clearly. Do not imply an inactive product was collected successfully.

### Schedule and run lists

Show the portal product name as the primary value and the stable code as secondary text. Do not expose Airflow DAG IDs in standard operator tables; show them only in authorized technical detail or the mapping-management screen.

The run detail must show the portal product, trading pair, mapped Airflow DAG ID, and catalog version used at execution time. This preserves traceability if the display name later changes.

## API additions

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/products` | List portal products and user-facing labels. |
| `GET` | `/api/v1/trading-pairs/{pair}/products` | Return the four-product operational summary for one pair. |
| `GET` | `/api/v1/product-mappings` | Administrator-only mapping list. |
| `PATCH` | `/api/v1/product-mappings/{id}` | Administrator-only portal-label/description update; DAG-ID changes use an explicit confirmation workflow. |

## Acceptance tests

1. A user sees exactly four configured product cards for an active trading pair.
2. Changing `Market Candles (OHLCV)` to a clearer portal name does not change the Airflow DAG ID or DAG behavior.
3. BTCUSDT can have one normal daily run for each of the four products.
4. A duplicate normal run for BTCUSDT + Candles + the same input day is rejected, while BTCUSDT + Trade Activity remains allowed.
5. The product name, product code, DAG ID, and catalog version are stored with the execution for audit traceability.
