# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Shared config
# MAGIC Every other notebook starts with `%run ./00_config`.
# MAGIC If your workspace's default catalog is not `workspace`, change CATALOG below.

# COMMAND ----------

CATALOG = "workspace"   # default catalog in Databricks Free Edition
SCHEMA = "game_analytics"
VOLUME = "raw"

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"


def _t(name):
    return f"{CATALOG}.{SCHEMA}.{name}"


# Bronze (raw, as-is + audit columns)
T_BRONZE_EVENTS = _t("bronze_events")
T_BRONZE_GAMES = _t("bronze_games")
T_BRONZE_PLAYERS = _t("bronze_players")

# Silver (clean, typed, deduplicated)
T_SILVER_EVENTS = _t("silver_events")
T_SILVER_REJECTS = _t("silver_rejects")
T_SILVER_GAMES = _t("silver_games")
T_SILVER_PLAYERS = _t("silver_players")

# Quality log
T_DQ = _t("dq_results")

# Gold (business-ready)
T_FACT_EVENTS = _t("fact_events")
T_DIM_GAME = _t("dim_game")
T_DIM_PLAYER = _t("dim_player")
T_GOLD_DAU = _t("gold_daily_active_users")
T_GOLD_REVENUE = _t("gold_daily_revenue_by_game")
T_GOLD_RETENTION = _t("gold_retention")
T_GOLD_TOP_GAMES = _t("gold_top_games_by_genre")

VALID_EVENT_TYPES = ["login", "logout", "match_start", "match_end", "purchase"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"Config loaded. Schema: {CATALOG}.{SCHEMA} | Volume path: {BASE}")
