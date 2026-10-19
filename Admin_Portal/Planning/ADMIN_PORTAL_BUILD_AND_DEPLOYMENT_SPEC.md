# Admin Portal: Build and Deployment Specification

Project: Financial Analytics and Fraud Detection System  
Status: implementation handoff  
Prepared: 2026-08-25  
Related plan: `Plan/LINKED_SYSTEMS_DATA_QUALITY_PIPELINE_PLAN.md`

## 1. Purpose and boundary

Build an **Admin Portal**: the internal operations workspace for collecting, generating, controlling, and maintaining data-pipeline tasks. It is not the business analytics dashboard (SYS-01), and it must not replace the specialist consoles (Airflow, MinIO, Grafana, MLflow, or Telegram).

The portal gives authorized company operators one reliable view of every task/batch and a controlled way to act on it. A task can be:

- **Collected**: created from a scheduled or manual data-collection request.
- **Automatically generated**: created by a rule, such as a retry, backfill, freshness recovery, or data-quality remediation proposal.
- **Manually created**: created by an authorized operator for a permitted task template.

The portal should show company-operational information, because that is its purpose. Show pipeline ownership, source, product/dataset, batch ID, status, quality, freshness, alerts, and audit history. Do **not** show secrets, API keys, passwords, raw personal data, full transaction payloads, or unrestricted infrastructure controls.

## 2. Product outcomes

The first production-ready release must let an operator:

1. See current health and work requiring attention without opening six systems.
2. Find any task or pipeline batch by batch ID, date, source, dataset, owner, or status.
3. Understand the separate states of collection, quality, transformation, Gold loading, and alerting.
4. Review why a failed or generated task exists, including source evidence and the recommended action.
5. Approve, reject, retry, cancel, or rerun only when their role permits it.
6. Open the relevant Airflow run, Grafana dashboard, MinIO object, ClickHouse result, or alert incident from the task detail.
7. Export filtered, non-sensitive operational history for a thesis report or audit evidence.
8. Preserve an immutable audit trail of every operator and system action.

## 3. Scope by release

### Release 1 — control-room MVP

- Single sign-in with role-based access control.
- Dashboard, task list, task detail, generated-task queue, quality/quarantine view, incident list, and audit log.
- Read model sourced from `pipeline_batch_status`, `data_quality_results`, and alert events.
- Safe task actions: retry, cancel queued task, acknowledge incident, approve/reject generated task.
- Deep links to the existing system consoles.
- CSV export and health/readiness endpoints.

### Release 2 — operational automation

- Rule engine to generate tasks for retry, stale-data recovery, and approved backfill templates.
- Approval policies, task assignment, comments, and notification routing.
- Quarantine review with remediation action templates.
- Saved filters, scheduled reports, and SLA tracking.

### Explicitly out of scope for Release 1

- Editing raw Bronze/Silver/Gold records in the portal.
- Directly exposing database shells, MinIO credentials, Airflow administrator credentials, or service tokens.
- Automatically rerunning destructive or high-cost jobs without policy approval.
- Replacing Airflow scheduling or Grafana monitoring.

## 4. Users and permissions

| Role | Typical user | Permissions |
| --- | --- | --- |
| Viewer | supervisor, thesis evaluator | View approved operational data, search, export permitted reports, open deep links. |
| Operator | data operations teammate | Viewer permissions; acknowledge alerts; create allowed manual tasks; retry/cancel own permitted tasks; add comments. |
| Approver | team lead | Operator permissions; approve/reject generated backfill, retry, and remediation tasks. |
| Administrator | platform maintainer | Manage users/roles, task templates, automation rules, integration configuration references, and retention settings. |
| Service account | Airflow/SYS-03/SYS-06 | Create/update tasks and incidents only through authenticated service APIs; cannot use the UI. |

Use least privilege. Approval must be separate from creation for risky task types. Every action requires the authenticated actor, timestamp, request/correlation ID, previous state, new state, and reason.

## 5. Core concepts and lifecycle

### Identifiers

- `task_id`: UUID created by the Admin Portal.
- `batch_id`: shared pipeline identifier, for example `binance_klines_BTCUSDT_20260822T090000Z`.
- `correlation_id`: identifier carried across portal, Airflow, SYS-03, and SYS-06 events.
- `airflow_dag_run_id`: Airflow-specific run identifier.

A task may represent a batch operation. A generated remediation task must retain links to the original failed `task_id` and `batch_id`.

### Task states

```text
Draft -> Pending approval -> Approved -> Queued -> Running -> Succeeded
                        \-> Rejected
Queued/Running -> Failed -> Generated remediation task
Queued -> Cancelled
```

`Failed` is terminal for that execution. A retry or rerun creates a new task execution linked to the original task; it must never overwrite history.

### Pipeline stage states

The detail page displays these independently:

| Stage | Allowed values |
| --- | --- |
| Collection | Pending, Queued, Running, Succeeded, Failed |
| Data Quality | Not started, Running, Passed, Failed, Waived (admin only, reason required) |
| Quarantine | Not required, Pending review, Stored, Resolved |
| Transform/Silver | Not started, Running, Succeeded, Blocked, Failed |
| Gold load | Not started, Running, Succeeded, Blocked, Failed |
| Alerting | None, Open, Acknowledged, Resolved |

Gold loading is permitted only when Data Quality is `Passed`. A waiver is an exceptional audited policy decision and should be disabled by default for thesis deployment.

## 6. Required screens

### A. Operations dashboard

Show the last 24 hours by default:

- KPI cards: running, failed, awaiting approval, quality failures, stale Gold datasets, open incidents.
- Trend: completed/failed tasks and quality pass rate by hour/day.
- Attention queue: failed, SLA-risk, stale, and approval-needed tasks.
- Pipeline freshness table: dataset/symbol, latest approved Gold timestamp, age, SLA target, health.
- System tiles with availability and deep links for SYS-01 through SYS-06.

### B. Task registry

One row per task execution. Required columns: task ID, batch ID, type, source, dataset/product, symbol, origin (schedule/manual/rule), owner, created time, current stage, overall status, quality status, priority, and action menu.

Required filters: date range, task type, origin, source/company, dataset/product, symbol, owner, overall status, quality status, priority, and incident presence. Include free-text search for task ID, batch ID, and correlation ID. Filters must be shareable via URL.

### C. Task detail

Use a chronological timeline and a stage summary. Include:

- Identity, task parameters, origin, triggering rule, parent task/batch, assignee, priority, and current status.
- Record counts for Bronze, rejected, Silver, and Gold.
- Quality metrics, failure summary, and links to the report/quarantine object.
- Airflow DAG/run link, MinIO path link, ClickHouse query/view link, alert incident link, and relevant logs.
- Permitted actions with confirmation, reason field where required, and clear consequence text.
- Comments and immutable audit events.

### D. Generated-task approval queue

Display rule-generated tasks before execution when approval is required. Include the rule name/version, triggering evidence, estimated impact, proposed parameters, parent failure, and recommended action. Approver can approve, reject, or request changes. Rejection must require a reason.

### E. Data quality and quarantine

Show failed checks, counts, sample redacted records, quarantine location, age, owner, and remediation state. Allow only safe actions: create a remediation task, mark reviewed, or escalate. Never allow portal users to delete quarantine files directly.

### F. Incidents and audit log

Incidents display severity, source, affected tasks, SLA, owner, acknowledgement, resolution note, and SYS-06 notification status. Audit log is searchable by actor, task/batch/correlation ID, action, and date; it is append-only and exportable only by authorized roles.

## 7. Integration architecture

```text
Airflow/SYS-02 ---- task + stage events ----\
SYS-05 ----------- batch/quality read data --- > Admin API -> PostgreSQL (portal data) -> Admin UI
SYS-03 ----------- health/incident events ----/
SYS-06 ----------- notification events -------/
                                                   -> Airflow control API (approved actions only)
                                                   -> Deep links: MinIO, ClickHouse, Grafana, MLflow
```

The portal is the **control plane and read model**, not the source of truth for raw market data. SYS-05 remains the source of data artifacts; Airflow remains the scheduler/executor.

### Data ownership

| Data | System of record | Portal use |
| --- | --- | --- |
| Raw, Silver, Gold, quarantine objects | SYS-05 / MinIO | Metadata and signed, time-limited download/view link where needed. |
| Quality and batch status | SYS-05 / ClickHouse | Read model and task-stage updates. |
| DAG execution | SYS-02 / Airflow | Trigger approved runs and show status. |
| Metrics/incidents | SYS-03/SYS-06 | Display, acknowledge, link, and correlate. |
| Users, roles, task definitions, approvals, comments, audit log | Admin Portal PostgreSQL | Portal owns these records. |

## 8. Data contract

### Minimum event envelope

All producers must publish the following JSON contract to the Admin API (direct HTTP initially; a message broker may be added later):

```json
{
  "event_id": "uuid",
  "event_type": "TASK_STAGE_UPDATED",
  "occurred_at": "2026-08-25T09:00:00Z",
  "producer": "sys-02",
  "correlation_id": "uuid",
  "task_id": "uuid-or-null",
  "batch_id": "binance_klines_BTCUSDT_20260822T090000Z",
  "payload": {}
}
```

Require `event_id` idempotency: receiving the same event twice must not duplicate a task, stage transition, incident, or audit entry.

### Portal PostgreSQL tables

Implement at least:

- `users`, `roles`, `user_roles`
- `task_templates`, `automation_rules`, `tasks`, `task_executions`, `task_stage_status`
- `task_links` (Airflow/MinIO/ClickHouse/Grafana/MLflow URLs and identifiers)
- `approvals`, `comments`, `audit_events`
- `incidents`, `incident_task_links`
- `integration_events` (raw envelope, deduplication status, processing error)

`tasks` stores the business request; `task_executions` stores each attempted run. Keep both `batch_id` and `correlation_id` indexed. Store timestamps in UTC.

### Required API endpoints

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/dashboard` | Dashboard summary and attention queues. |
| `GET/POST` | `/api/v1/tasks` | Search tasks / create allowed manual task. |
| `GET` | `/api/v1/tasks/{taskId}` | Detail, stages, links, comments, audit events. |
| `POST` | `/api/v1/tasks/{taskId}/approve` | Approve an approval-gated task. |
| `POST` | `/api/v1/tasks/{taskId}/reject` | Reject with reason. |
| `POST` | `/api/v1/tasks/{taskId}/retry` | Create linked retry if policy permits. |
| `POST` | `/api/v1/tasks/{taskId}/cancel` | Cancel only before unsafe execution stage. |
| `GET` | `/api/v1/quality-results` | Filtered quality/quarantine data. |
| `GET` | `/api/v1/incidents` | Incident list/detail. |
| `POST` | `/api/v1/incidents/{id}/acknowledge` | Record acknowledgement. |
| `POST` | `/api/v1/events` | Authenticated idempotent producer ingestion. |
| `GET` | `/healthz`, `/readyz` | Liveness/readiness for deployment. |

Use OpenAPI as the implementation contract. Validate every mutation server-side; the UI must never be trusted for authorization or state transitions.

## 9. Automation policy

Start with versioned rules configured by an administrator. Each rule has a name, version, enabled flag, trigger condition, action template, risk level, and approval policy.

| Rule | Trigger | Generated task | Approval |
| --- | --- | --- | --- |
| Retry transient collection | API/network failure and attempts remain | Retry same batch | Automatic only within retry limit; otherwise Approver. |
| Stale Gold recovery | Gold age exceeds dataset SLA | Recollect/reprocess scoped time window | Approver. |
| Quality remediation | Quality failure with known recoverable cause | Quarantine review or re-collection | Approver. |
| Backfill | Authorized request with defined dates/symbols | Backfill pipeline | Approver; Administrator for broad range. |
| Model retrain | New quality-approved Gold data meets threshold | SYS-04 retrain request | Automatic only after SYS-04 policy check. |

Automation must never directly delete data, bypass Data Quality, disable alerts, rotate secrets, or run unlimited backfills. Add rate limits, maximum date range, maximum concurrent runs, and a kill switch per rule.

## 10. Security and operational requirements

- Use SSO/OIDC where available; for thesis deployment, use a secure local account provider with hashed passwords, MFA for admins if feasible, and short-lived sessions.
- Put the portal behind HTTPS and a reverse proxy. Do not expose Airflow, MinIO, ClickHouse, PostgreSQL, Prometheus, or Grafana directly to the public internet.
- Replace all checked-in/demo credentials such as `admin123` before cloud deployment. Store secrets only in deployment environment variables or a secret manager; never in the UI bundle, source control, logs, or exports.
- Service-to-service event calls use a distinct service identity and signed token/API key with rotation process.
- Log authentication, authorization denial, state changes, approvals, exports, and integration failures. Redact tokens and sensitive payload values.
- Apply pagination, date-range limits, API rate limits, input validation, CSRF protection for cookie sessions, and secure headers.
- Retain audit data at least through thesis presentation; define data/quarantine retention separately in SYS-05.
- Backup portal PostgreSQL daily and test restore before deployment freeze.

## 11. Recommended implementation stack

- **Frontend:** React + TypeScript + Vite, with a responsive data-table component and chart library.
- **Backend:** FastAPI + Pydantic + SQLAlchemy/Alembic, producing OpenAPI documentation.
- **Portal database:** PostgreSQL (a dedicated database/schema; do not reuse Airflow metadata tables).
- **Authentication:** OIDC-compatible provider; a simple local provider only for initial thesis demo if no IdP is available.
- **Deployment:** Docker multi-stage images, Docker Compose profile `admin-portal`, Nginx/Caddy reverse proxy, and Cloudflare Tunnel or VM firewall/reverse-proxy access.

The current root `portal.html` is a useful visual prototype. Treat it as reference only; do not place credentials in its page content and do not use static sample rows as the production data source.

## 12. Build plan and acceptance criteria

### Milestone 1: foundation

Build the API, schema migrations, authentication, role middleware, audit logger, health endpoints, and Docker configuration.

Acceptance: unauthenticated requests are denied; each role sees only permitted actions; `/readyz` checks PostgreSQL and required integration configuration.

### Milestone 2: read-only operations view

Build dashboard, task registry, detail page, quality view, incident view, and SYS-02/SYS-05 event adapters. Seed development data only through fixtures.

Acceptance: an Airflow batch event appears once; its quality result and status are searchable by batch ID; links resolve correctly; a failed quality gate shows Gold as blocked.

### Milestone 3: controlled actions and automation

Build manual task creation from templates, approval queue, retry/cancel actions, rule engine, comments, and CSV export.

Acceptance: an approver can approve a bounded backfill; the portal triggers exactly one Airflow run; retry creates a linked execution; every action has an audit event; a non-approver cannot perform approval.

### Milestone 4: deployment rehearsal

Deploy the portal with the shared cloud Compose network, production secrets, HTTPS, backups, logs, and monitoring.

Acceptance: restart recovery works; portal data restores from backup in rehearsal; external access reaches only the reverse proxy; a failure event creates an incident and SYS-06 notification; the health script passes.

## 13. Testing requirements

- Unit: state-transition policy, RBAC, event idempotency, automation rule evaluation, input validation.
- Integration: SYS-02 event -> portal task -> quality result -> approved retry -> Airflow trigger; quality failure -> quarantine link -> incident.
- End-to-end: Viewer search; Operator acknowledgement; Approver approval; Administrator rule change; denied unauthorized action.
- Security: secret scan, dependency scan, authentication/authorization tests, audit-log redaction test.
- Performance: task registry with at least 10,000 rows paginates and filters within agreed target (recommend under 2 seconds in thesis environment).
- Backup/restore: restore a portal PostgreSQL backup to a clean instance and verify tasks, approvals, and audits.

## 14. Deployment runbook

1. Create a dedicated portal PostgreSQL database and least-privilege application user.
2. Add production values to a non-committed `.env` file: database URL, session/OIDC secrets, service-event credential, allowed origins, public base URL, deep-link base URLs, and backup location.
3. Build and scan frontend/backend images; run migrations as a one-time deploy job.
4. Start the `admin-portal` Compose profile on the shared Docker network after SYS-05 and before public exposure.
5. Configure the reverse proxy for HTTPS, `/api`, SPA routing, request-size limits, and security headers.
6. Configure SYS-02, SYS-03, SYS-05, and SYS-06 event endpoints and verify one signed test event from each required producer.
7. Configure daily PostgreSQL backup and verify a restore once.
8. Run smoke tests: sign in; view task; receive event; approve permitted task; verify audit; verify health endpoints.
9. Put the system behind monitoring and alert on portal unavailability, event-processing failures, failed backups, database space, and elevated API errors.
10. Before presentation, freeze schema/API changes, rotate demo credentials, and test local/Oracle backup recovery according to the cloud plan.

## 15. Teammate deliverables checklist

- [ ] `admin-portal/frontend` React application with the required screens.
- [ ] `admin-portal/backend` FastAPI service with OpenAPI, RBAC, audit, event ingestion, and action adapters.
- [ ] Alembic migrations and development fixtures.
- [ ] Dockerfiles, Compose profile, reverse-proxy configuration, `.env.example`, and a secrets inventory (names only).
- [ ] SYS-02/SYS-03/SYS-05/SYS-06 event-contract adapters and contract tests.
- [ ] Unit, integration, end-to-end, security, and restore-rehearsal test evidence.
- [ ] Operator runbook, deployment runbook, rollback steps, and troubleshooting guide.
- [ ] Architecture diagram and screenshots for the thesis report.

## 16. Definition of done

The Admin Portal is ready for teammate handoff and thesis deployment when a protected user can trace a batch from collection through quality, quarantine/Silver/Gold, incident, and alert; safely approve or act on permitted generated tasks; see complete audit history; and recover the portal from a tested backup without exposing system credentials or bypassing the Data Quality gate.
