# Agent guide

Context for coding agents (Claude Code, Genie Code, Cursor) working in this bundle.

## What this is

A teaching bundle: a monthly revenue adjustment CSV becomes daily revenue rows, reconciled to the file and compared with a legacy table. See README.md for the business rules and the expected results.

## Rules the code must keep

- **Year:** the file's modification year, minus one when the file's month is later than the arrival month.
- **Spread:** `adjustment_total / days_in_month` as `decimal(19,4)`; the last day carries the remainder so each month adds back exactly.
- **Replace:** per month, only the latest file containing that month counts (`max_by(file_id, struct(source_modified_at, file_id))`).
- **Lookup:** customers map to revenue types; a customer with no lookup row is `UNMAPPED` in the reconciliation, never silently dropped.
- **Types are explicit.** Never infer CSV types; cast in code.
- **Bad files stay visible.** Expectations warn; they don't drop or fail.
- **`raw_adjustment_files` keeps `pipelines.reset.allowed = false`.**

## Where things go

- Pipeline objects: `src/pipeline/` (Python for ingestion, SQL for transforms). Unqualified names resolve to the pipeline's catalog and schema.
- New jobs, alerts, dashboards: a YAML file in `resources/`, named `<name>.<resource_type>.yml`.
- Names that change by target come from variables: `${var.catalog}` and `${resources.schemas.simple_build.name}`. Never hard-code a catalog or schema.

## How to work

1. Read the relevant file and README section before changing anything.
2. Show the change and explain it in two or three sentences before deploying.
3. `databricks bundle validate -t <target>`, then `deploy`, then `run`.
4. Prove it with a query; compare with `checks/expected_results.sql`.

## Don't

- Don't write to `landing/incoming/` except to drop a scenario file on purpose.
- Don't change the legacy table to make the compare pass; that hides the point it makes.
- Don't add secrets, tokens or real customer data.
