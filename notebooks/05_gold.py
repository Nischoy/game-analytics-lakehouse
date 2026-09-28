# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Gold layer: star schema + business metrics
# MAGIC Gold tables are rebuilt with `CREATE OR REPLACE TABLE ... AS SELECT` from Silver, so they
# MAGIC are always consistent (late-arriving events are picked up automatically).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------
# MAGIC %md ## Star schema

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {T_DIM_GAME} AS
SELECT game_id, game_name, genre, base_price FROM {T_SILVER_GAMES}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {T_DIM_PLAYER} AS
SELECT player_id, signup_date, country, platform FROM {T_SILVER_PLAYERS}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {T_FACT_EVENTS} AS
SELECT event_id, player_id, game_id, event_type, event_ts, event_date, amount
FROM {T_SILVER_EVENTS}
""")

# COMMAND ----------
# MAGIC %md ## Daily active users

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {T_GOLD_DAU} AS
SELECT
  event_date,
  COUNT(DISTINCT player_id)                 AS dau,
  COUNT(*)                                  AS total_events,
  COUNT_IF(event_type = 'match_start')      AS matches_played
FROM {T_FACT_EVENTS}
GROUP BY event_date
ORDER BY event_date
""")

# COMMAND ----------
# MAGIC %md ## Revenue by game by day

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {T_GOLD_REVENUE} AS
SELECT
  f.event_date,
  f.game_id,
  g.game_name,
  g.genre,
  COUNT(*)                 AS purchases,
  ROUND(SUM(f.amount), 2)  AS revenue
FROM {T_FACT_EVENTS} f
JOIN {T_DIM_GAME} g ON f.game_id = g.game_id
WHERE f.event_type = 'purchase'
GROUP BY f.event_date, f.game_id, g.game_name, g.genre
""")

# COMMAND ----------
# MAGIC %md ## Retention (cohort = first day a player was seen)
# MAGIC Retention % is NULL when the cohort is too recent for that window to have completed.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {T_GOLD_RETENTION} AS
WITH first_seen AS (
  SELECT player_id, MIN(event_date) AS cohort_date
  FROM {T_FACT_EVENTS} GROUP BY player_id
),
activity AS (
  SELECT DISTINCT player_id, event_date FROM {T_FACT_EVENTS}
),
max_d AS (
  SELECT MAX(event_date) AS max_date FROM {T_FACT_EVENTS}
),
agg AS (
  SELECT
    f.cohort_date,
    COUNT(DISTINCT f.player_id) AS cohort_size,
    COUNT(DISTINCT CASE WHEN a.event_date = date_add(f.cohort_date, 1) THEN f.player_id END) AS d1_retained,
    COUNT(DISTINCT CASE WHEN a.event_date = date_add(f.cohort_date, 7) THEN f.player_id END) AS d7_retained
  FROM first_seen f
  JOIN activity a ON a.player_id = f.player_id
  GROUP BY f.cohort_date
)
SELECT
  agg.cohort_date, agg.cohort_size, agg.d1_retained, agg.d7_retained,
  CASE WHEN date_add(agg.cohort_date, 1) <= m.max_date
       THEN ROUND(agg.d1_retained * 100.0 / agg.cohort_size, 1) END AS d1_retention_pct,
  CASE WHEN date_add(agg.cohort_date, 7) <= m.max_date
       THEN ROUND(agg.d7_retained * 100.0 / agg.cohort_size, 1) END AS d7_retention_pct
FROM agg CROSS JOIN max_d m
ORDER BY agg.cohort_date
""")

# COMMAND ----------
# MAGIC %md ## Top 3 games per genre by revenue

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {T_GOLD_TOP_GAMES} AS
SELECT genre, game_name, players, revenue, rank_in_genre
FROM (
  SELECT
    genre, game_name, players, revenue,
    ROW_NUMBER() OVER (PARTITION BY genre ORDER BY revenue DESC) AS rank_in_genre
  FROM (
    SELECT
      g.genre,
      g.game_name,
      COUNT(DISTINCT f.player_id) AS players,
      ROUND(COALESCE(SUM(CASE WHEN f.event_type = 'purchase' THEN f.amount END), 0), 2) AS revenue
    FROM {T_FACT_EVENTS} f
    JOIN {T_DIM_GAME} g ON f.game_id = g.game_id
    GROUP BY g.genre, g.game_name
  )
)
WHERE rank_in_genre <= 3
""")

# COMMAND ----------

display(spark.table(T_GOLD_DAU).orderBy("event_date"))
