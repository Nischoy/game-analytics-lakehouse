# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze layer: land raw data as-is
# MAGIC - Events: Auto Loader (incremental - only NEW files are processed on each run)
# MAGIC - Games / players: small reference files, full refresh
# MAGIC
# MAGIC Rule: **no cleaning in Bronze.** Every column is a string; we only add audit columns.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------
# MAGIC %md ## Events via Auto Loader

# COMMAND ----------

event_schema = StructType([
    StructField(c, StringType(), True)
    for c in ["event_id", "player_id", "game_id", "event_type", "event_time", "amount"]
])

raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(event_schema)
    .load(f"{BASE}/events/")
)

bronze_stream = raw_stream.select(
    "*",
    F.col("_metadata.file_path").alias("source_file"),
    F.current_timestamp().alias("ingested_at"),
)

(
    bronze_stream.writeStream
    .option("checkpointLocation", f"{BASE}/_checkpoints/bronze_events")
    .trigger(availableNow=True)      # process everything new, then stop (batch-style streaming)
    .toTable(T_BRONZE_EVENTS)
    .awaitTermination()
)

print("bronze_events rows:", spark.table(T_BRONZE_EVENTS).count())

# COMMAND ----------
# MAGIC %md ## Reference data (games, players)

# COMMAND ----------

for name, table in [("games", T_BRONZE_GAMES), ("players", T_BRONZE_PLAYERS)]:
    (
        spark.read.option("header", True).csv(f"{BASE}/{name}/{name}.csv")
        .withColumn("ingested_at", F.current_timestamp())
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    )
    print(table, spark.table(table).count())
