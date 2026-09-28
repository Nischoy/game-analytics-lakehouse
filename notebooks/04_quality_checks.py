# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Data quality gate
# MAGIC Runs between Silver and Gold. If any check fails the notebook raises an error,
# MAGIC the job task fails, and Gold is NOT refreshed with bad data.
# MAGIC Results are logged to `dq_results` for auditing.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

checks = []


def add(name, passed, detail):
    checks.append((name, bool(passed), str(detail)))


silver = spark.table(T_SILVER_EVENTS)
total = silver.count()
bronze_n = spark.table(T_BRONZE_EVENTS).count()
rejects_n = spark.table(T_SILVER_REJECTS).count()

add("silver_not_empty", total > 0, f"rows={total}")

null_keys = silver.where(
    "event_id IS NULL OR player_id IS NULL OR event_ts IS NULL OR event_type IS NULL"
).count()
add("no_null_keys", null_keys == 0, f"null_rows={null_keys}")

dupes = total - silver.select("event_id").distinct().count()
add("no_duplicate_event_ids", dupes == 0, f"duplicates={dupes}")

bad_types = silver.where(~F.col("event_type").isin(VALID_EVENT_TYPES)).count()
add("valid_event_types", bad_types == 0, f"bad_rows={bad_types}")

bad_purchase = silver.where("event_type = 'purchase' AND (amount IS NULL OR amount <= 0)").count()
add("purchase_amount_positive", bad_purchase == 0, f"bad_rows={bad_purchase}")

stray_amount = silver.where("event_type <> 'purchase' AND amount IS NOT NULL").count()
add("amount_only_on_purchases", stray_amount == 0, f"bad_rows={stray_amount}")

orphans = (
    silver.where("game_id IS NOT NULL")
    .join(spark.table(T_SILVER_GAMES), "game_id", "left_anti")
    .count()
)
add("game_ids_exist_in_dim", orphans == 0, f"orphan_rows={orphans}")

add(
    "row_reconciliation",
    total + rejects_n <= bronze_n,
    f"bronze={bronze_n}, silver={total}, rejects={rejects_n}, duplicates_dropped~={bronze_n - total - rejects_n}",
)

reject_rate = rejects_n / bronze_n if bronze_n else 0
add("reject_rate_under_5pct", reject_rate < 0.05, f"reject_rate={reject_rate:.2%}")

# COMMAND ----------

results = (
    spark.createDataFrame(checks, "check_name string, passed boolean, detail string")
    .withColumn("run_ts", F.current_timestamp())
)
results.write.mode("append").saveAsTable(T_DQ)
display(results)

failed = [c for c in checks if not c[1]]
if failed:
    raise Exception(f"DATA QUALITY FAILED: {failed}")
print("All data quality checks passed.")
