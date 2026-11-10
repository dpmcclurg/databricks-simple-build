-- Expected results for the base sample. Run in the SQL editor after simple_build_setup.
-- Set your catalog and schema first (dev copies use dev_<user>_simple_build).
USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

-- 1. Ledger: 1 file, 7 rows, no parse_error
SELECT source_file_name, row_count, parse_error FROM adjustment_files;

-- 2. Parsed: month 10 = 4 rows, 21,833.2500; month 11 = 3 rows, 16,680.8500; adjustment_total is decimal(19,4)
SELECT month, COUNT(*) AS rows, SUM(adjustment_total) AS total, first(typeof(adjustment_total)) AS column_type
FROM adjustments_parsed GROUP BY ALL ORDER BY month;

-- 3. Spread: 214 rows; every revenue type and month adds back exactly to the file
SELECT COUNT(*) AS rows FROM revenue_adjustment_daily;
SELECT revenue_type_code, CAST(date_trunc('MONTH', revenue_date) AS DATE) AS month,
       COUNT(*) AS days, SUM(revenue_amount) AS total
FROM revenue_adjustment_daily GROUP BY ALL ORDER BY ALL;
-- Expected: 901 Oct 12,500.0000 · 902 Oct 3,175.5000 · 903 Oct -842.2500 · 904 Oct 7,000.0000
--           901 Nov 13,250.0000 · 902 Nov 2,980.7500 · 905 Nov 450.1000

-- 4. Remainder rule: type 902 in October is 102.4355 on days 1 to 30 and 102.4350 on the 31st
SELECT day(revenue_date) AS day, revenue_amount
FROM revenue_adjustment_daily
WHERE revenue_type_code = 902 AND month(revenue_date) = 10 AND day(revenue_date) IN (1, 30, 31)
ORDER BY day;

-- 5. Reconciliation: 7 rows, all OK
SELECT status, COUNT(*) FROM adjustment_reconciliation GROUP BY ALL;

-- 6. Legacy compare: one row per month, matched = false, status COMPARED
SELECT * FROM legacy_compare ORDER BY month_start;

-- 7. Metric view: 2026-10 = 21,833.25; 2026-11 = 16,680.85
SELECT `Month`, MEASURE(`Revenue`) AS revenue FROM mv_revenue_adjustment_daily GROUP BY ALL ORDER BY ALL;

-- 8. Outbound: reconciliation_2026-10.csv (4 rows) and reconciliation_2026-11.csv (3 rows)
LIST '/Volumes/<catalog>/<schema>/outbound/';
