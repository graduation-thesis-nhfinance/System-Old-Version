# Admin Portal Addendum: Search, Airflow Links, and Downloadable Artifacts

Related specifications:

- `Plan/ADMIN_PORTAL_BUILD_AND_DEPLOYMENT_SPEC.md`
- `Plan/ADMIN_PORTAL_TRADING_PAIR_PRODUCT_CATALOG_ADDENDUM.md`

## Confirmed decisions

1. Operators can search and inspect tasks by trading pair and collection product.
2. Airflow opens in a **new browser tab**; it is not embedded in the Admin Portal.
3. Authorized Admin Portal users can download a task's uploaded JSON artifact/file to inspect locally.

“Everyone” in this document means every authenticated user who has permission to see the selected task and artifact. Files must not be publicly downloadable merely because a URL is copied.

## Recommended portal product names

These are clear for non-technical operators while retaining a precise label in parentheses:

| # | Recommended display name | Secondary label | Purpose |
| --- | --- | --- | --- |
| 1 | **Price Candles** | OHLCV market data | Price and volume for each time interval. |
| 2 | **Trade Activity** | Aggregated trades | Executed trade activity for the pair. |
| 3 | **Order Book** | Bid/ask depth snapshots | Available buy/sell depth at collection time. |
| 4 | **Liquidation Events** | Forced liquidation data | Forced-close events used in risk analysis. |

Portal labels are display-only catalog values. The existing Airflow DAG IDs remain unchanged and are stored in the protected product-to-DAG mapping.

## Search and navigation requirements

### Task explorer

Provide a primary **Task Explorer** page. Required filters:

- trading pair, for example `BTCUSDT`;
- product: Price Candles, Trade Activity, Order Book, or Liquidation Events;
- input-day range and schedule timezone;
- normal run or recollection attempt;
- Collect, Data Quality, Transform, Gold, and overall status;
- batch ID, task ID, Airflow DAG ID, or Airflow DAG-run ID;
- error/incident presence.

Selecting a result opens **Pipeline Details**. The breadcrumb must remain clear:

```text
Trading Pair -> Product -> Input Day -> Attempt -> Pipeline Details
BTCUSDT     -> Price Candles -> 2026-09-08 -> Normal -> batch_id
```

## Pipeline Details requirements

Replicate the useful structure of the company reference, but make it clearer:

- top summary: pair, portal product, input day, batch ID, normal/recollection attempt, overall state, owner, start/end time, and quality-approved authoritative batch;
- stage panels: Collect, Data Quality, Transform, Gold Load, and Alerting;
- each panel: state, created/updated time, duration, event count, concise message/error, and `View trace`;
- event table: event type, task ID, actor/service, artifact(s), Airflow action, error/message, and created time;
- expandable, sanitized event metadata for authorized technical users;
- an obvious `Open in Airflow` action wherever a DAG run exists.

Use a timeline or stage progress bar at the top so a non-technical reviewer can see the completed/blocked stage before reading the event table. Keep raw JSON collapsed by default.

## Airflow new-tab linking

The backend supplies a validated Airflow DAG-run URL for the relevant task. The UI opens it with `target="_blank"` and `rel="noopener noreferrer"`.

Requirements:

- show the link only when an authenticated user may view the corresponding task;
- label it `Open in Airflow` rather than `Check`;
- include the DAG ID and DAG-run ID in a tooltip/technical detail;
- do not put Airflow credentials or session tokens in the URL;
- the Airflow instance retains its own authentication and authorization;
- record an audit event when an operator follows the link, with task/batch ID and actor—not a token or URL query secrets.

## Uploaded JSON/artifact download

Each artifact shown in a trace displays a user-facing filename, type, size, upload time, checksum where available, and source stage. The action is **Download JSON** (or `Download file` for non-JSON artifacts), not an exposed storage path.

Download flow:

1. User clicks an artifact in Pipeline Details.
2. Admin API checks the task/artifact permission and audit policy.
3. API returns a short-lived, single-artifact download URL or streams the authorized object.
4. Browser downloads the file with `Content-Disposition: attachment`.
5. Portal records actor, artifact ID, batch ID, time, and outcome in the audit log.

Security requirements:

- Store the internal MinIO/S3 path separately; do not reveal bucket credentials or permanent object URLs in the browser.
- Use short-lived signed URLs (recommend 5 minutes) only after authorization.
- Validate the artifact belongs to the selected task/batch before creating a link.
- Enforce a configurable maximum download size; show a warning for large files.
- Malware/format scanning is recommended before marking an uploaded file available.
- Do not render arbitrary JSON/HTML directly in the portal. If preview is added later, render a size-limited, escaped, redacted text view.

## API additions

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/tasks` | Search with `trading_pair`, `product_code`, `input_day`, status, and Airflow identifiers. |
| `GET` | `/api/v1/tasks/{taskId}/trace` | Pipeline Details and stage/event trace. |
| `GET` | `/api/v1/tasks/{taskId}/airflow-link` | Authorize and return validated new-tab DAG-run link. |
| `POST` | `/api/v1/artifacts/{artifactId}/download` | Authorize and issue short-lived download response/URL. |

## Acceptance tests

1. Searching `BTCUSDT` plus `Price Candles` returns only matching executions; filtering by the same pair and `Trade Activity` returns a different valid result set.
2. A Pipeline Details page shows all stages and ordered events, including task IDs, timestamps, artifact references, and messages/errors.
3. `Open in Airflow` opens a new tab for the correct DAG run without embedding Airflow or sharing portal credentials.
4. An authorized user downloads a linked JSON artifact; the downloaded file matches its recorded checksum.
5. A user without permission cannot obtain a download URL, and a copied signed URL fails after expiry.
6. Airflow-link follows and artifact downloads appear in the Admin Portal audit log.
