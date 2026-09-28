# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Run the whole pipeline (fallback if you don't use Workflows)
# MAGIC Order: bronze -> silver -> quality gate -> gold. Any failure stops the chain.

# COMMAND ----------

for nb in ["02_bronze", "03_silver", "04_quality_checks", "05_gold"]:
    print(f"Running {nb} ...")
    dbutils.notebook.run(f"./{nb}", 3600)
    print(f"{nb} OK")
