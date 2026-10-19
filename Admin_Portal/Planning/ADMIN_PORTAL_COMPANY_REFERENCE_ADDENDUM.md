# Admin Portal Addendum: Daily Collection and Pipeline Trace

Related base specification: `Plan/ADMIN_PORTAL_BUILD_AND_DEPLOYMENT_SPEC.md`  
Source of requirements: company portal reference screens reviewed 2026-08-25

## Decision

Use the company portal's operating flow as the Admin Portal's core workflow:

```text
Collect schedules -> Pipeline runs -> Pipeline trace
```

Each flow level has a distinct meaning:

- **Collect schedules** is configuration: one row per active schedule.
- **Pipeline runs** is history: one row per execution for a selected schedule and input day.
- **Pipeline trace** is evidence: chronological, stage-level events for one execution.

## One-collection-per-day policy

The default business policy is **one normal collection per day** for a unique collection key:

```text
(schedule_id, seller_or_source_id, product_id, symbol, input_day)
```

For a daily `D-1` schedule, a 07:30 run on 2026-08-25 collects the input day 2026-08-24. Timezone must come from the schedule, not the browser or server default.

### Enforcement

1. The backend creates a daily lock before triggering Airflow.
2. A unique database index prevents two `NORMAL` runs with the same collection key.
3. A repeated request returns `409 Conflict` with the existing task ID, batch ID, and status.
4. The UI disables **Run now** and explains that the day's normal collection already exists.
5. The same check is performed in the Airflow trigger adapter; never rely on the button state alone.

### Recollection exceptions

A recollection is allowed only as a linked, separately numbered execution. It never overwrites the normal run or its trace.

| Situation | Recollect? | Required control |
| --- | --- | --- |
| Normal run succeeded | No | View its trace/artifact instead. |
| Normal run queued/running | No | Prevent duplicate execution. |
| Collection failed before valid artifact | Yes, one recovery attempt | Operator action or bounded automatic retry. |
| Data Quality failed | Yes | Approver, reason, and quarantine link required. |
| Source corrected or arrived late | Yes | Approver and reason `LATE_OR_CORRECTED_SOURCE`. |
| Backfill date range | Yes | Approver/Admin; range and concurrency limits. |

Default limit: **one approved recollection per key per day**. A further exception requires Administrator approval plus an incident/reference number.

Every pipeline-run row must display: `Normal` or `Recollection #n`, reason, requester, approver, parent batch, and authoritative quality-approved batch. This prevents two same-day runs from appearing as unrelated duplicates.

## Screen requirements from the reference

### Collect schedules

Include schedule ID, seller/source, company, product, platform/system instance, cron, human-readable schedule, timezone, input-day scope, active state, last normal run, next run, daily state, and actions.

Actions: create, edit, activate/deactivate, run now, view runs, and export. `Run now` must explain why it is blocked, e.g. “Input day 2026-08-24 was already collected; batch …”.

### Pipeline runs

Filter by schedule ID, input-day range, status, seller/source, company, and product. Include collect, Data Quality, transform, Gold, and overall status separately. Default sort: newest input day, then newest attempt.

### Pipeline trace

Group events into Collect, Data Quality, Transform, Gold load, and Alerting. For each stage, show state, timestamps, task/DAG-run link, artifact link, concise message/error, and event count.

The trace may offer expandable metadata, but it must sanitize the payload. Do not show connection IDs, secret names/values, signed object URLs, credentials, personal emails, or unneeded raw file content. Full trace access must be role-gated and audited.

## Schema/API additions

Add:

- `collect_schedules`
- `collection_daily_locks`
- `recollection_requests`
- `schedule_id`, `input_day`, `execution_kind`, `attempt_number`, `parent_execution_id`, `recollection_reason`, and `approved_by` to the execution model

Required endpoints:

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/collect-schedules` | Search schedules and daily state. |
| `POST` | `/api/v1/collect-schedules/{id}/run` | Request normal collection or a policy-gated recollection. |
| `GET` | `/api/v1/collect-schedules/{id}/runs` | List traces/runs for a schedule. |

Add a dashboard card for scheduled today, collected once, pending, failed, recollected, and duplicate requests blocked.

## Acceptance test

For one active daily schedule and one input day, send two normal-run requests concurrently. Exactly one execution and one Airflow DAG run must result. The other request must return the first task/batch ID. An approved recollection must receive a new execution/batch ID, remain linked to the original, and appear in the trace without changing original audit events.
