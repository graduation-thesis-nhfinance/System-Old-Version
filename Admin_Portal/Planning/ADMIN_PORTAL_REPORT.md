# Admin Portal Report

Project: Financial Analytics and Fraud Detection System  
Status: teammate build and deployment handoff  
Last updated: 2026-09-10

## Purpose

The Admin Portal is the internal operations workspace for maintaining collection schedules, finding pipeline tasks, reviewing pipeline execution, and opening the linked Airflow run when a deeper technical check is required.

It is a control and visibility layer. Airflow remains the scheduler and task executor; SYS-05 remains the storage and data-result owner.

## Confirmed collection scope

The portal shows six configured market symbols and four collection products for every symbol: **24 product slots**.

| Configured symbol | Market Price Candles (OHLCV) | Trade Activity | Order Book Depth | Liquidation Events |
| --- | --- | --- | --- | --- |
| `BTCUSDT` | Show status | Show status | Show status | Show status |
| `ETHUSDT` | Show status | Show status | Show status | Show status |
| `BNBUSDT` | Show status | Show status | Show status | Show status |
| `AAPLBUSDT` | Show status | Show status | Show status | Show status |
| `AMZNBUSDT` | Show status | Show status | Show status | Show status |
| `DELLBUSDT` | Show status | Show status | Show status | Show status |

The portal display names are approved as follows:

1. Market Price Candles (OHLCV)
2. Trade Activity
3. Order Book Depth
4. Liquidation Events

These are Admin Portal labels only. They do not rename Airflow DAGs, tasks, or behavior.

## Honest availability policy

Every one of the 24 slots must be visible. A configured symbol/product must never be hidden and must never be represented as successfully collected when it is unavailable.

Supported status values:

- `Collected once`, `Queued`, `Running`, `Done`, `Failed`
- `Unavailable from source`
- `Product not yet supported`

Current implementation note: SYS-02 DAG `04_binance_market_metrics_15m_dag` configures all six symbols, but its current Liquidation Events collector streams the first three configured symbols only. The other three Liquidation Events slots must be shown as `Product not yet supported` until Airflow is expanded.

## Daily collection and recollection

The normal policy is one collection per schedule, trading pair, product, and input day. A daily schedule normally collects `D-1` using its configured timezone.

Collection key:

```text
(schedule_id, trading_pair, product_id, input_day)
```

A second normal run is blocked and returns the existing task/batch. A recollection is a new linked execution with a reason, requester, approver where required, and a retained original trace. Default limit: one approved recollection per key per day.

## Required user journey

```text
Collect schedule -> Trading pair + product -> Pipeline run -> Pipeline Details -> Airflow DAG run
```

1. Operator views configured schedules and their daily state.
2. Operator searches by pair, product, input day, status, batch ID, task ID, or Airflow run ID.
3. Operator opens the matching Pipeline Details record.
4. Operator checks stages, events, files, errors, and task timings.
5. Operator opens the corresponding Airflow DAG run in a new tab if deeper investigation is needed.

## Required screens

### Control Room

- Daily collection compliance: six configured pairs, 24 product slots, collected once, attention needed, unavailable, and duplicate requests blocked.
- Attention queue for failed, stale, approval-needed, and unavailable tasks.
- Pair coverage summary with four product statuses per pair.

### Task Explorer

- Filters: trading pair, product, input-day range, status, attempt, batch ID, task ID, and Airflow DAG/DAG-run ID.
- One row per execution, clearly identifying normal run or recollection attempt.
- Direct action to open Pipeline Details.

### Collect Schedules

- Schedule ID, pair, portal product name, timezone, cron, human-readable timing, input-day scope, active state, last run, next run, daily state, and permitted actions.
- `Run now` is disabled with a reason if the daily normal run already exists.

### Pipeline Details

- Summary: pair, product, input day, attempt, batch ID, owner, overall state, and authoritative quality-approved batch.
- Separate stages: Collect, Data Quality, Transform, Gold Load, Alerting.
- Every stage: state, created/updated time, duration, event count, concise message/error, and trace link.
- Ordered event rows: event type, task ID, actor/service, artifact, Airflow action, error/message, and created time.
- Sanitized expandable metadata; secret values, connection IDs, signed URLs, and unnecessary personal data are redacted.

### Product Catalog

- Portal display name, explanation, quality profile, active state, catalog version, and protected mapped Airflow DAG ID.
- Administrators may change display names/descriptions. DAG-ID mapping changes require confirmation, audit reason, and validation.

## Airflow and artifact access

- Provide `Open in Airflow` for each task with an Airflow DAG run.
- Open Airflow in a new browser tab using `target="_blank"` and `rel="noopener noreferrer"`.
- Airflow keeps its own authentication; portal credentials or tokens must never be placed in the URL.
- Show uploaded artifact filename, type, size, upload time, checksum where available, and source stage.
- Authorized users can click `Download JSON`/`Download file` to inspect locally.
- Download access is authorized server-side and uses a short-lived signed URL or authenticated stream. Do not expose permanent MinIO/S3 URLs or credentials.
- Audit every Airflow-link follow and artifact download.

## Build acceptance criteria

1. The portal renders six pairs and 24 product slots.
2. An unsupported or unavailable slot is explicit and never reported as successful.
3. Search for pair + product returns only matching runs.
4. A Pipeline Details page shows all stages and ordered events.
5. An authorized user can open the correct Airflow DAG run in a new tab.
6. An authorized user can download an authorized JSON artifact; an unauthorized user cannot obtain a download URL.
7. Two normal-run requests for the same daily collection key create one execution only.
8. An approved recollection creates a linked new execution and does not overwrite the original trace.

## Reference documents

- `ADMIN_PORTAL_BUILD_AND_DEPLOYMENT_SPEC.md`
- `ADMIN_PORTAL_COMPANY_REFERENCE_ADDENDUM.md`
- `ADMIN_PORTAL_TRADING_PAIR_PRODUCT_CATALOG_ADDENDUM.md`
- `ADMIN_PORTAL_TRACE_AND_ARTIFACT_ACCESS_ADDENDUM.md`
- `ADMIN_PORTAL_SIX_TRADING_PAIR_SCOPE.md`
