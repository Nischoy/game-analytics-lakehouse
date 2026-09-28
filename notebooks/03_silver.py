# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver layer: clean, type, validate, deduplicate
# MAGIC Design decisions (say these in interviews):
# MAGIC - **Incremental**: only Bronze rows newer than what Silver has already seen.
# MAGIC - **Idempotent**: `MERGE ... WHEN NOT MATCHED` on event_id, so reruns never create duplicates.
# MAGIC - **Quarantine, don't drop**: bad rows go to `silver_rejects` with a reason.
# MAGIC - **ANSI-safe parsing**: `try_to_timestamp` / `try_cast` return NULL instead of crashing.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F, Window

# COMMAND ----------
# MAGIC %md ## 1. Pick up only new Bronze rows

# COMMAND ----------

if spark.catalog.tableExists(T_SILVER_EVENTS):
    src = spark.sql(f"""
        SELECT * FROM {T_BRONZE_EVENTS}
        WHERE ingested_at > (
            SELECT COALESCE(MAX(bronze_ingested_at), TIMESTAMP'1970-01-01') FROM {T_SILVER_EVENTS}
        )
    """)
else:
    src = spark.table(T_BRONZE_EVENTS)

print("Bronze rows to process:", src.count())

# COMMAND ----------
# MAGIC %md ## 2. Parse, standardise and flag invalid rows

# COMMAND ----------

parsed = (
    src
    .withColumn("player_id_c", F.when(F.trim("player_id") == "", None).otherwise(F.trim("player_id")))
    .withColumn("event_type_c", F.lower(F.trim("event_type")))
    .withColumn(
        "event_ts",
        F.coalesce(
            F.expr("try_to_timestamp(event_time, 'yyyy-MM-dd HH:mm:ss')"),
            F.expr("try_to_timestamp(event_time, 'dd/MM/yyyy HH:mm:ss')"),
        ),
    )
    .withColumn("amount_c", F.expr("try_cast(amount AS DOUBLE)"))
    .withColumn(
        "reject_reason",
        F.when(F.col("event_id").isNull(), "missing_event_id")
        .when(F.col("player_id_c").isNull(), "missing_player_id")
        .when(F.col("event_type_c").isNull() | ~F.col("event_type_c").isin(VALID_EVENT_TYPES), "invalid_event_type")
        .when(F.col("event_ts").isNull(), "unparseable_timestamp")
        .when(
            (F.col("event_type_c") == "purchase")
            & (F.col("amount_c").isNull() | (F.col("amount_c") <= 0)),
            "invalid_purchase_amount",
        ),
    )
)

# COMMAND ----------
# MAGIC %md ## 3. Quarantine rejects

# COMMAND ----------

rejects = parsed.where(F.col("reject_reason").isNotNull()).select(
    "event_id", "player_id", "game_id", "event_type", "event_time", "amount",
    "reject_reason", "source_file",
    F.col("ingested_at").alias("bronze_ingested_at"),
    F.current_timestamp().alias("rejected_at"),
)
rejects.write.mode("append").saveAsTable(T_SILVER_REJECTS)

# COMMAND ----------
# MAGIC %md ## 4. Deduplicate valid rows and MERGE into Silver

# COMMAND ----------

w = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())

clean = (
    parsed.where(F.col("reject_reason").isNull())
    .withColumn("rn", F.row_number().over(w))
    .where("rn = 1")
    .select(
        "event_id",
        F.col("player_id_c").alias("player_id"),
        "game_id",
        F.col("event_type_c").alias("event_type"),
        "event_ts",
        F.to_date("event_ts").alias("event_date"),
        F.when(F.col("event_type_c") == "purchase", F.col("amount_c")).alias("amount"),
        "source_file",
        F.col("ingested_at").alias("bronze_ingested_at"),
        F.current_timestamp().alias("silver_processed_at"),
    )
)

# create empty table on first run (no-op if it already exists)
clean.limit(0).write.mode("ignore").saveAsTable(T_SILVER_EVENTS)

clean.createOrReplaceTempView("clean_events")
spark.sql(f"""
    MERGE INTO {T_SILVER_EVENTS} AS t
    USING clean_events AS s
    ON t.event_id = s.event_id
    WHEN NOT MATCHED THEN INSERT *
""")

print("silver_events:", spark.table(T_SILVER_EVENTS).count())
print("silver_rejects:", spark.table(T_SILVER_REJECTS).count())

# COMMAND ----------
# MAGIC %md ## 5. Reference tables

# COMMAND ----------

(
    spark.table(T_BRONZE_GAMES)
    .select(
        F.trim("game_id").alias("game_id"),
        F.trim("game_name").alias("game_name"),
        F.trim("genre").alias("genre"),
        F.expr("try_cast(base_price AS DOUBLE)").alias("base_price"),
    )
    .dropDuplicates(["game_id"])
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_SILVER_GAMES)
)

(
    spark.table(T_BRONZE_PLAYERS)
    .select(
        F.trim("player_id").alias("player_id"),
        F.expr("try_cast(signup_date AS DATE)").alias("signup_date"),
        "country",
        "platform",
    )
    .dropDuplicates(["player_id"])
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_SILVER_PLAYERS)
)

# COMMAND ----------
# MAGIC %md ## Reject breakdown (screenshot this for your README)

# COMMAND ----------

display(spark.sql(f"""
    SELECT reject_reason, COUNT(*) AS rows
    FROM {T_SILVER_REJECTS} GROUP BY reject_reason ORDER BY rows DESC
"""))
