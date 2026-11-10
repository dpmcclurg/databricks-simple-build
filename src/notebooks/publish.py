# Databricks notebook source
# MAGIC %md
# MAGIC # Publish: outbound files and the metric view
# MAGIC
# MAGIC 1. Writes one reconciliation CSV per month to the `outbound` Volume (the confirmation that
# MAGIC    goes back to accounting).
# MAGIC 2. Creates the metric view on the daily table: measures defined once, grouped at query time.

# COMMAND ----------

import os

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fq = f"{catalog}.{schema}"
outbound = f"/Volumes/{catalog}/{schema}/outbound"

# COMMAND ----------

recon = spark.table(f"{fq}.adjustment_reconciliation").orderBy("year_month", "customer_id").toPandas()
for f in os.listdir(outbound):
    if f.startswith("reconciliation_") and f.endswith(".csv"):
        os.remove(f"{outbound}/{f}")
for ym, rows in recon.groupby("year_month"):
    rows.to_csv(f"{outbound}/reconciliation_{ym}.csv", index=False)
print(sorted(os.listdir(outbound)))

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq}.mv_revenue_adjustment_daily
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Daily revenue adjustments. Totals by month add back exactly to the adjustment file.
source: {fq}.revenue_adjustment_daily
joins:
  - name: revenue_type
    source: {fq}.customer_revenue_type
    on: source.revenue_type_code = revenue_type.revenue_type_code
dimensions:
  - name: Month
    expr: DATE_TRUNC('MONTH', revenue_date)
    comment: Calendar month of the revenue day.
  - name: Revenue day
    expr: revenue_date
    comment: The day the revenue is booked to.
  - name: Revenue type code
    expr: revenue_type_code
    comment: Revenue type from the customer lookup.
  - name: Revenue type
    expr: revenue_type.revenue_type_name
    comment: Revenue type name.
measures:
  - name: Revenue
    expr: SUM(revenue_amount)
    comment: Sum of daily revenue amounts.
  - name: Days
    expr: COUNT(DISTINCT revenue_date)
    comment: Number of days with revenue.
  - name: Average daily revenue
    expr: MEASURE(Revenue) / MEASURE(Days)
    comment: Revenue divided by days, at whatever grain you group by.
$$
""")
display(spark.sql(f"SELECT `Month`, MEASURE(`Revenue`) AS revenue FROM {fq}.mv_revenue_adjustment_daily GROUP BY ALL ORDER BY ALL"))
