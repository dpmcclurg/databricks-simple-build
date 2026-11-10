# databricks-simple-build

A small, complete Databricks build for engineers who are new to the platform. A monthly revenue adjustment file lands in a folder, and the bundle turns it into daily revenue rows, proves the rows add back to the file, compares them with a legacy table, and reports on all of it. Everything is one [Declarative Automation Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/), so the whole build is source code you can read, change and redeploy.

All data is synthetic.

## The business problem

Accounting drops a CSV each month: one row per customer with a total and a month number.

```csv
customer_id,adjustment_total,month
90001,12500.00,10
90002,3175.50,10
```

Reporting needs one row per revenue type per day, so each monthly total is spread over the days of its month. Three rules make that harder than it looks:

- **The file has no year.** The year comes from when the file arrived. A January file that holds December belongs to the previous year.
- **Even spreads don't add up.** 3,175.50 over 31 days is 102.4355 a day at four decimals, and 31 of those make 3,175.5005. The last day of the month carries the remainder (102.4350), so the month adds back exactly.
- **A new file replaces the month.** For each month, the latest file that contains it wins. A customer left out of a corrected file drops out of that month.

## What gets built

```
landing/incoming/*.csv ──► Lakeflow pipeline ──────────────────────────────► simple_build_run job
   (Volume)                 raw_adjustment_files   one row per file version     outbound/*.csv
                            adjustment_files       the file ledger              mv_revenue_adjustment_daily
                            adjustments_parsed     typed rows
                            customer_revenue_type  the lookup
                            revenue_adjustment_daily     one row per revenue type per day
                            adjustment_reconciliation    file total = spread total?
                            legacy_compare               exact match with legacy_fact_revenue
                                     │
                                     └──► 2 SQL alerts · ops dashboard
```

| Piece | What it is | Why it's built this way |
|---|---|---|
| **Volume** `landing` | A governed folder in Unity Catalog | Files get the same permissions model as tables |
| **Auto Loader** (`binaryFile`) | A streaming reader for a folder that remembers which files it has read | Each file is loaded once, restarts resume where they stopped, and a corrected file with the same name is picked up again |
| `raw_adjustment_files` | Streaming table, one row per file version | `pipelines.reset.allowed = false`, so a full refresh can't erase the record of what arrived |
| `adjustment_files` | Materialized view: the file ledger | One row per file with its row count and `parse_error`; bad files stay visible instead of disappearing |
| `adjustments_parsed` | Materialized view: typed rows | Types come from code (`decimal(19,4)`), not inference, which would guess `double` |
| Expectations | Data quality rules in the pipeline | Set to warn: bad rows are counted, not dropped |
| `revenue_adjustment_daily` | Materialized view: the daily spread | The three rules above, in plain SQL (`src/pipeline/transforms.sql`) |
| `adjustment_reconciliation` | Pass/fail check | File total vs spread total per customer and month: `OK`, `MISMATCH` or `UNMAPPED` (no lookup row) |
| `legacy_compare` | Exact comparison with `legacy_fact_revenue` | Every column cast to string, compared both ways, counts only. No tolerance: 0.0001 off is a mismatch |
| SQL alerts | Queries with a condition | A reconciliation break doesn't fail a job, so the alert is the signal |
| Dashboard | AI/BI dashboard | Files loaded and failed, rows per month, reconciliation, legacy compare |
| `mv_revenue_adjustment_daily` | Metric view | Measures (`Revenue`, `Days`, `Average daily revenue`) defined once, grouped however you query them |

The legacy table holds no rows for the sample's revenue types (901 to 905), so `legacy_compare` reports `matched = false`. That is expected: it shows how the comparison works and why the reconciliation against the file is the pass/fail check.

## Deploy

You need the Databricks CLI (1.20 or later) and an existing Unity Catalog catalog you can create schemas in. On a Databricks field (FEVM) workspace, that's the catalog that comes with it, `serverless_stable_<id>_catalog`. The bundle can't create a catalog there: the metastore has no storage root, and the API asks for a storage location.

```bash
databricks bundle deploy -t fevm --var catalog=<catalog>   # the shared copy
databricks bundle run    -t fevm --var catalog=<catalog> simple_build_setup

databricks bundle deploy -t dev  --var catalog=<catalog>   # your personal copy: [dev <you>] names, dev_<you>_ schema
databricks bundle run    -t dev  --var catalog=<catalog> simple_build_setup
```

The setup job copies the sample files into the Volume, builds the legacy table, runs the pipeline, then writes the outbound files and the metric view. It takes about three minutes. After that, `simple_build_run` picks up whatever is in `landing/incoming/`.

To skip `--var` on every command, put `{"catalog": "<catalog>"}` in `.databricks/bundle/<target>/variable-overrides.json`.

## Check it worked

Open `checks/expected_results.sql` in the SQL editor, set `catalog` and `schema`, and run it. The base sample gives:

| Check | Expected |
|---|---|
| Ledger | 1 file, 7 rows, no `parse_error` |
| Parsed | Month 10: 4 rows, 21,833.25. Month 11: 3 rows, 16,680.85 |
| Daily spread | 214 rows (4 × 31 + 3 × 30), every month adds back exactly |
| Remainder | Type 902 in October: 102.4355 on days 1 to 30, 102.4350 on the 31st |
| Reconciliation | 7 rows, all `OK` |
| Legacy compare | `matched = false` for both months |
| Metric view | 2026-10: 21,833.25. 2026-11: 16,680.85 |
| Outbound | `reconciliation_2026-10.csv` (4 rows), `reconciliation_2026-11.csv` (3 rows) |

## Try a scenario

Scenario files are copied to `landing/scenarios/`. Copy one into `landing/incoming/`, run `simple_build_run`, and look at the dashboard.

| File | What happens |
|---|---|
| `revised_month.csv` | November is replaced: customer 90001 changes, and 90005's November rows disappear. October doesn't change |
| `bad_header.csv` | The ledger records a `parse_error`, the file adds no rows, and the parse-error alert triggers |
| `unmapped_customer.csv` | A full October file plus customer 90006, who has no lookup row: reconciliation shows `UNMAPPED` and the break alert triggers |
| The original sample, copied again | The file is read again as a new version, and totals don't change |

To start over: `databricks bundle destroy -t <target>`, then deploy and run setup again.

## Layout

```
databricks.yml                   variables and targets (fevm, dev)
resources/                       schema and volumes, warehouse, pipeline, jobs, alerts, dashboard
src/pipeline/ingestion.py        Auto Loader, the ledger, typed rows, the lookup
src/pipeline/transforms.sql      daily spread, reconciliation, legacy compare
src/notebooks/seed.py            sample files and the legacy table
src/notebooks/publish.py         outbound CSVs and the metric view
src/dashboards/                  the ops dashboard
data/                            sample, lookup, scenarios
checks/expected_results.sql      what "working" looks like
```

## License

MIT
