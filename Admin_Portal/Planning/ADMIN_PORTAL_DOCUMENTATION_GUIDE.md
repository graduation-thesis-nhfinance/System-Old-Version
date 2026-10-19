# Admin Portal Documentation Guide

Status: active navigation guide  
Last updated: 2026-09-13

## Start here

**Primary build document:** `ADMIN_PORTAL_REPORT.md`

Every teammate should start with this file. It defines the current agreed scope: six configured trading pairs, four portal products per pair, 24 visible product slots, daily collection/recollection policy, required UI screens, Airflow links, artifact downloads, and build acceptance criteria.

**Technical implementation appendix:** `ADMIN_PORTAL_BUILD_AND_DEPLOYMENT_SPEC.md`

Read this after the main report when implementing the backend, database, security, event contract, automation, tests, Docker configuration, and deployment.

## Document map

| File | Purpose | Use it when |
| --- | --- | --- |
| `ADMIN_PORTAL_REPORT.md` | Current product scope and implementation handoff. | Starting any Admin Portal work; deciding what to build. |
| `ADMIN_PORTAL_BUILD_AND_DEPLOYMENT_SPEC.md` | Detailed technical design: roles, lifecycle, API, database, integrations, security, tests, and deployment. | Designing or coding backend/frontend integration and deployment. |
| `ADMIN_PORTAL_COMPANY_REFERENCE_ADDENDUM.md` | Requirements derived from the company portal flow: schedules -> runs -> trace, one normal collection per day, and recollection controls. | Building schedule, run-history, trace, duplicate prevention, or recollection behavior. |
| `ADMIN_PORTAL_TRADING_PAIR_PRODUCT_CATALOG_ADDENDUM.md` | Portal-owned product catalog and portal-name-to-Airflow-DAG mapping rules. | Building product configuration, schedule templates, or pair/product UI. |
| `ADMIN_PORTAL_APPROVED_PRODUCT_NAMES.md` | Final approved display names for the four data products. | Showing product labels in UI, reports, and demonstrations. |
| `ADMIN_PORTAL_TRACE_AND_ARTIFACT_ACCESS_ADDENDUM.md` | Search, Pipeline Details, secure new-tab Airflow links, and authorized JSON/file downloads. | Building Task Explorer, trace detail, artifact download, or Airflow navigation. |
| `ADMIN_PORTAL_SIX_TRADING_PAIR_SCOPE.md` | Confirmed six-pair and 24-slot scope, including unsupported/unavailable status rules. | Implementing six-pair views and truthful source/product availability. |
| `admin-portal-prototype/` | First UI exploration with three layout variants. | Comparing layout ideas; not production implementation. |
| `admin-portal-prototype-six-pairs/` | Corrected visible UI demo for six pairs and 24 product slots. | Reviewing the latest UI behavior and giving design feedback. |

## Reading order for a teammate

1. `ADMIN_PORTAL_REPORT.md`
2. `ADMIN_PORTAL_BUILD_AND_DEPLOYMENT_SPEC.md`
3. The specific addendum for the feature being implemented.
4. `admin-portal-prototype-six-pairs/index.html` for UI behavior and terminology.

## Source-of-truth rule

When documents appear to conflict, use this priority order:

1. `ADMIN_PORTAL_REPORT.md` — current product decisions.
2. The newest explicit decision record, especially `ADMIN_PORTAL_SIX_TRADING_PAIR_SCOPE.md` and `ADMIN_PORTAL_APPROVED_PRODUCT_NAMES.md`.
3. `ADMIN_PORTAL_BUILD_AND_DEPLOYMENT_SPEC.md` — technical detail that does not conflict with current decisions.
4. Earlier addenda — feature-specific explanation and acceptance tests.

No document is deleted or moved by this organization pass.
