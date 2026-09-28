# Game Analytics Lakehouse

End-to-end **data engineering pipeline on Databricks**: simulated game telemetry flows through a
**medallion architecture (Bronze → Silver → Gold)** with incremental loads, idempotent upserts,
automated data-quality gates and a published dashboard.

> Built with: Databricks (Free Edition), PySpark, Delta Lake, Auto Loader, Databricks Workflows, SQL, Python

## Why this project
<2-3 sentences: you wanted to practise production-style pipeline patterns (incremental, idempotent, tested)
on a domain you care about: games.>

## Architecture
![architecture](docs/architecture.png)

| Layer | Tables | What happens |
|---|---|---|
| Bronze | `bronze_events`, `bronze_games`, `bronze_players` | Raw data landed as-is via Auto Loader, plus audit columns |
| Silver | `silver_events`, `silver_rejects`, `silver_games`, `silver_players` | Typed, standardised, deduplicated. Bad rows quarantined with a reason |
| Quality gate | `dq_results` | 9 automated checks; failure stops the job before Gold |
| Gold | `fact_events`, `dim_game`, `dim_player`, `gold_daily_active_users`, `gold_retention`, `gold_daily_revenue_by_game`, `gold_top_games_by_genre` | Star schema + business marts |

## Data
Python generator produces ~<N>K events across <14> days for <60,000> players and <40> games, with
**deliberately injected problems**: duplicates (2%), null player IDs, misspelled event types,
mixed timestamp formats, invalid purchase amounts, late-arriving events.

## Key engineering decisions
- **Incremental ingestion**: Auto Loader with `availableNow` trigger; only new files are processed.
- **Idempotency**: Silver uses `MERGE ... WHEN NOT MATCHED` on `event_id`; reruns never duplicate data.
- **Quarantine over deletion**: invalid rows land in `silver_rejects` with `reject_reason` (<X>% reject rate).
- **ANSI-safe parsing**: `try_to_timestamp` / `try_cast` so bad values become NULL instead of crashing the job.
- **Fail-fast quality gate**: the workflow halts if any check fails, so Gold never publishes bad data.
- **Rebuildable Gold**: `CREATE OR REPLACE TABLE ... AS SELECT` so late data is always reflected.

## Results
- <N> raw events → <N> clean events, <N> rejected, <N> duplicates removed
- Pipeline runs daily via Databricks Workflows with retries and failure alerts

![dashboard](docs/dashboard.png)
![workflow](docs/workflow_dag.png)
![dq](docs/dq_results.png)

## How to run
1. Create a Databricks Free Edition workspace; import `notebooks/` (or add this repo as a Git folder).
2. Run `01_generate_data` to create the raw files.
3. Run the workflow (or `06_run_all`): bronze → silver → quality checks → gold.
4. Open the dashboard / query the `gold_*` tables.
5. To test incremental loads: run `01_generate_data` with `start_date=2026-09-15`, `num_days=1`, then rerun the workflow.

## Repo structure
```
notebooks/
  00_config.py           shared names and paths
  01_generate_data.py    simulated source systems (dirty data)
  02_bronze.py           Auto Loader ingestion
  03_silver.py           clean, validate, dedupe, MERGE
  04_quality_checks.py   quality gate
  05_gold.py             star schema and marts
  06_run_all.py          fallback runner
docs/                    diagram and screenshots
```

## What I would do next
- Replace the file generator with Kafka + Structured Streaming
- Liquid clustering on `event_date`; incremental Gold instead of full rebuild
- CI with pytest and Databricks Asset Bundles for deployment
- Churn-prediction model (XGBoost) on a player-features table
