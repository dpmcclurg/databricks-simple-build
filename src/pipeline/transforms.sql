-- Daily spread, reconciliation and legacy compare.
-- Unqualified names resolve to the pipeline's catalog and schema.

-- Rules:
--   Year:    the file's modification year, minus one when the file's month is
--            later than the arrival month (a January file holding December).
--   Spread:  total / days in month at 4 decimals; the last day carries the
--            remainder so every month adds back to the file exactly.
--   Replace: for each month, only the latest file containing that month counts.
--   Lookup:  customers become revenue types; the daily table has no customer column.

CREATE OR REFRESH MATERIALIZED VIEW revenue_adjustment_daily
COMMENT 'One row per revenue type per day, in the legacy table''s columns and types.'
AS
WITH parsed AS (
  SELECT p.*,
         year(p.source_modified_at)
           - CASE WHEN p.month > month(p.source_modified_at) THEN 1 ELSE 0 END AS adj_year
  FROM adjustments_parsed AS p
),
latest_file AS (
  SELECT adj_year, month, max_by(file_id, struct(source_modified_at, file_id)) AS file_id
  FROM parsed
  GROUP BY adj_year, month
),
counted AS (
  SELECT p.* FROM parsed AS p JOIN latest_file AS l USING (adj_year, month, file_id)
),
months AS (
  SELECT lk.revenue_type_code,
         c.adjustment_total,
         make_date(c.adj_year, c.month, 1)                AS month_start,
         day(last_day(make_date(c.adj_year, c.month, 1))) AS days_in_month
  FROM counted AS c
  JOIN customer_revenue_type AS lk ON lk.customer_id = c.customer_id
),
days AS (
  SELECT *,
         explode(sequence(month_start, last_day(month_start)))   AS revenue_day,
         CAST(adjustment_total / days_in_month AS DECIMAL(19,4)) AS daily_amount
  FROM months
)
SELECT CAST(revenue_day AS TIMESTAMP) AS revenue_date,
       CASE WHEN revenue_day = last_day(month_start)
            THEN CAST(adjustment_total - daily_amount * (days_in_month - 1) AS DECIMAL(19,4))
            ELSE daily_amount
       END                           AS revenue_amount,
       revenue_type_code,
       9                             AS revenue_category_code,
       0                             AS is_forecast
FROM days;


CREATE OR REFRESH MATERIALIZED VIEW adjustment_reconciliation
COMMENT 'Pass/fail: file total vs spread total per customer and month. UNMAPPED means no lookup row.'
AS
WITH parsed AS (
  SELECT p.*,
         year(p.source_modified_at)
           - CASE WHEN p.month > month(p.source_modified_at) THEN 1 ELSE 0 END AS adj_year
  FROM adjustments_parsed AS p
),
latest_file AS (
  SELECT adj_year, month, max_by(file_id, struct(source_modified_at, file_id)) AS file_id
  FROM parsed
  GROUP BY adj_year, month
),
counted AS (
  SELECT p.* FROM parsed AS p JOIN latest_file AS l USING (adj_year, month, file_id)
),
spread AS (
  SELECT revenue_type_code,
         CAST(date_trunc('MONTH', revenue_date) AS DATE) AS month_start,
         SUM(revenue_amount) AS spread_total
  FROM revenue_adjustment_daily
  GROUP BY ALL
)
SELECT c.customer_id,
       lk.revenue_type_code,
       date_format(make_date(c.adj_year, c.month, 1), 'yyyy-MM') AS year_month,
       c.source_file_name,
       c.adjustment_total                               AS file_total,
       coalesce(s.spread_total, 0)                      AS spread_total,
       c.adjustment_total - coalesce(s.spread_total, 0) AS difference,
       CASE WHEN lk.customer_id IS NULL THEN 'UNMAPPED'
            WHEN c.adjustment_total = coalesce(s.spread_total, 0) THEN 'OK'
            ELSE 'MISMATCH' END                         AS status
FROM counted AS c
LEFT JOIN customer_revenue_type AS lk ON lk.customer_id = c.customer_id
LEFT JOIN spread AS s
  ON s.revenue_type_code = lk.revenue_type_code
 AND s.month_start = make_date(c.adj_year, c.month, 1);


CREATE OR REFRESH MATERIALIZED VIEW legacy_compare
COMMENT 'Exact match against the legacy table per month, both directions, counts only. No tolerance.'
AS
WITH ours AS (
  SELECT CAST(date_trunc('MONTH', revenue_date) AS DATE) AS month_start,
         CAST(revenue_date AS STRING) AS d, CAST(revenue_amount AS STRING) AS a,
         CAST(revenue_type_code AS STRING) AS r, CAST(revenue_category_code AS STRING) AS c,
         CAST(is_forecast AS STRING) AS f
  FROM revenue_adjustment_daily
),
months AS (SELECT DISTINCT month_start FROM ours),
legacy AS (
  SELECT m.month_start,
         CAST(l.revenue_date AS STRING) AS d, CAST(l.revenue_amount AS STRING) AS a,
         CAST(l.revenue_type_code AS STRING) AS r, CAST(l.revenue_category_code AS STRING) AS c,
         CAST(l.is_forecast AS STRING) AS f
  FROM legacy_fact_revenue AS l
  JOIN months AS m
    ON l.revenue_date >= m.month_start AND l.revenue_date < add_months(m.month_start, 1)
  WHERE l.revenue_category_code = 9 AND l.is_forecast = 0
),
only_ours   AS (SELECT * FROM ours   EXCEPT ALL SELECT * FROM legacy),
only_legacy AS (SELECT * FROM legacy EXCEPT ALL SELECT * FROM ours),
n_ours        AS (SELECT month_start, count(*) AS n FROM ours        GROUP BY month_start),
n_legacy      AS (SELECT month_start, count(*) AS n FROM legacy      GROUP BY month_start),
n_only_ours   AS (SELECT month_start, count(*) AS n FROM only_ours   GROUP BY month_start),
n_only_legacy AS (SELECT month_start, count(*) AS n FROM only_legacy GROUP BY month_start)
SELECT m.month_start,
       coalesce(o.n, 0)  AS ours,
       coalesce(l.n, 0)  AS legacy,
       coalesce(oo.n, 0) AS only_ours,
       coalesce(ol.n, 0) AS only_legacy,
       coalesce(l.n, 0) > 0 AND coalesce(oo.n, 0) = 0 AND coalesce(ol.n, 0) = 0 AS matched,
       CASE WHEN coalesce(l.n, 0) = 0 THEN 'NO_LEGACY_ROWS' ELSE 'COMPARED' END AS status
FROM months AS m
LEFT JOIN n_ours        AS o  USING (month_start)
LEFT JOIN n_legacy      AS l  USING (month_start)
LEFT JOIN n_only_ours   AS oo USING (month_start)
LEFT JOIN n_only_legacy AS ol USING (month_start);
