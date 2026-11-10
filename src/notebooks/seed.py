# Databricks notebook source
# MAGIC %md
# MAGIC # Seed: sample files and the legacy table
# MAGIC
# MAGIC Copies the sample adjustment file into the drop folder (only if the folder is empty, so a
# MAGIC rerun never undoes a scenario), the lookup next to it, and the scenario files into
# MAGIC `landing/scenarios/`. Then builds `legacy_fact_revenue`: a synthetic stand-in for an old
# MAGIC warehouse table, generated deterministically so every run gives the same rows.

# COMMAND ----------

import os
import shutil

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("data_dir", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
data_dir = dbutils.widgets.get("data_dir")
if not data_dir.startswith("/Workspace"):
    data_dir = "/Workspace" + data_dir

landing = f"/Volumes/{catalog}/{schema}/landing"
incoming = f"{landing}/incoming"
scenarios = f"{landing}/scenarios"
os.makedirs(incoming, exist_ok=True)
os.makedirs(scenarios, exist_ok=True)

shutil.copy(f"{data_dir}/customer_revenue_type.csv", f"{landing}/customer_revenue_type.csv")
for name in os.listdir(f"{data_dir}/scenarios"):
    shutil.copy(f"{data_dir}/scenarios/{name}", f"{scenarios}/{name}")
if not [f for f in os.listdir(incoming) if f.endswith(".csv")]:
    shutil.copy(f"{data_dir}/revenue_adjustments_sample.csv", f"{incoming}/revenue_adjustments_sample.csv")

print("incoming:", sorted(os.listdir(incoming)))
print("scenarios:", sorted(os.listdir(scenarios)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The legacy table
# MAGIC
# MAGIC About 90,000 rows: revenue types 1 to 144, categories 0 to 9 (some null), dates from 2014 to
# MAGIC 2038 (future-dated rows included, as old warehouses often have). It holds no rows for the
# MAGIC sample's revenue types (901 to 905), so the legacy compare reports a mismatch for the reason
# MAGIC a real migration often does: the new codes don't exist in the old system.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.legacy_fact_revenue
COMMENT 'Synthetic legacy warehouse fact. Read only by convention; the compare reads it.'
AS
SELECT CAST(id + 1 AS BIGINT) AS revenue_id,
       CAST(date_add(DATE'2014-08-01', CAST(pmod(hash(id, 1), 8590) AS INT)) AS TIMESTAMP) AS revenue_date,
       CAST((pmod(hash(id, 2), 2000000) - 50000) / 1000.0 AS DECIMAL(19,4)) AS revenue_amount,
       CAST(1 + pmod(hash(id, 3), 144) AS INT) AS revenue_type_code,
       CASE WHEN pmod(hash(id, 4), 10) = 0 THEN NULL ELSE CAST(pmod(hash(id, 5), 10) AS INT) END AS revenue_category_code,
       CASE WHEN pmod(hash(id, 6), 33) = 0 THEN NULL ELSE 0 END AS is_forecast
FROM range(90000)
""")
display(spark.sql(f"SELECT count(*) AS rows, min(revenue_date), max(revenue_date) FROM {catalog}.{schema}.legacy_fact_revenue"))
