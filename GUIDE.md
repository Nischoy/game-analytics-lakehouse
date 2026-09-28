# 12-Hour Build Guide: Game Analytics Lakehouse

Everything is already coded in `notebooks/`. Your job: run it, understand it, screenshot it, document it.
Realistic time: about 6-7 hours of doing plus 3-4 hours of understanding and README, which fits your 12.

Free Edition facts (checked today): serverless compute only, Jobs allowed (max 5 concurrent tasks),
one 2X-Small SQL warehouse, limited internet access. This project is designed around those limits.

---

## PHASE 0 - Setup (30-45 min)

1. **Databricks Free Edition**: sign up at databricks.com (search "Databricks Free Edition"). Pick serverless when asked.
2. **GitHub repo**: create a new public repo `game-analytics-lakehouse` (add a README, or leave empty).
3. **Get the notebooks into Databricks** (choose one):
   - **Option A (recommended, also gives you Git history):** push this folder to GitHub, then in Databricks go to
     Workspace, then Create, then Git folder, paste your repo URL. Notebooks appear as real notebooks.
   - **Option B (fastest):** Workspace, then Create, then Folder named `game_analytics`. Open it, then
     the three-dot menu, then Import, and upload each `.py` file from `notebooks/`. The header line
     `# Databricks notebook source` makes each import as a notebook.
4. Open `00_config`. If your default catalog is not `workspace`, change `CATALOG`. (Catalog Explorer in the sidebar shows your catalogs.)

Push commands (from this folder on your laptop):
```
git init
git add .
git commit -m "Initial: medallion pipeline notebooks"
git branch -M main
git remote add origin https://github.com/<you>/game-analytics-lakehouse.git
git push -u origin main
```
**Commit again after every phase.** A commit history looks far better than one giant upload.

---

## PHASE 1 - Generate data (30 min)

1. Open `01_generate_data`, attach **Serverless**, click **Run all**.
2. Expect roughly 14 lines like `2026-09-05: 61,234 events -> /Volumes/...`, about 500k-800k events total. Takes a few minutes.
3. Verify in Catalog Explorer: `workspace` > `game_analytics` > `raw` (volume) > `events`, `games`, `players`.
4. Open one event file and read it. Spot the null `player_id`, `"N/A"` amounts, `"LOGIN "` with a trailing space, and `dd/MM/yyyy` timestamps.
   That is the messy data your pipeline will fix. Mention it in interviews.

**Understand:** the data has a realistic retention curve (players return less as days pass), so your retention chart will look real.

---

## PHASE 2 - Bronze (30 min)

1. Run `02_bronze`.
2. Verify with a new SQL cell or the SQL editor:
```sql
SELECT COUNT(*) FROM workspace.game_analytics.bronze_events;
SELECT * FROM workspace.game_analytics.bronze_events LIMIT 20;
```
3. You should see all columns as strings plus `source_file`, `ingested_at`.

**Understand (interview material):**
- **Auto Loader** (`cloudFiles`) remembers which files it has processed via the checkpoint, so it only ingests NEW files.
- `trigger(availableNow=True)` means "process everything new, then stop". Streaming semantics, batch cost.
- Bronze keeps raw strings on purpose: never lose source data, and you can reprocess Silver anytime.

**Test it now (2 min):** run `02_bronze` again. The row count must not change. That is incremental ingestion working.

---

## PHASE 3 - Silver (60-90 min, the most important phase)

1. Run `03_silver`. Read the printed counts and the reject breakdown at the bottom.
2. Verify:
```sql
SELECT * FROM workspace.game_analytics.silver_events LIMIT 20;
SELECT reject_reason, COUNT(*) FROM workspace.game_analytics.silver_rejects GROUP BY 1;
-- silver + rejects should be below bronze; the gap is duplicates that were dropped
SELECT
 (SELECT COUNT(*) FROM workspace.game_analytics.bronze_events) AS bronze,
 (SELECT COUNT(*) FROM workspace.game_analytics.silver_events) AS silver,
 (SELECT COUNT(*) FROM workspace.game_analytics.silver_rejects) AS rejects;
```
3. **Screenshot** the reject breakdown.

**Understand (be able to explain every line):**
| Concept | Where | Why |
|---|---|---|
| Watermark filter | top of the notebook | Process only new Bronze rows: incremental |
| `try_to_timestamp`, `try_cast` | parsing step | On serverless (ANSI mode) plain `to_timestamp` or `cast` **errors** on bad data; `try_` returns NULL |
| `coalesce` of two formats | `event_ts` | Handles the mixed timestamp formats |
| Rejects table | step 3 | Quarantine with a reason, never silently drop |
| `row_number()` dedupe | step 4 | Removes duplicate events within a batch |
| `MERGE ... WHEN NOT MATCHED` | step 4 | **Idempotency**: rerunning never inserts an event_id twice |

**Idempotency test (do this, it is your best interview story):** run `03_silver` again. Counts must NOT change.

---

## PHASE 4 - Quality gate (30 min)

1. Run `04_quality_checks`. All checks should show `passed = true`.
2. Screenshot the results table.
3. **Prove the gate works (screenshot this too):** temporarily insert a bad row and rerun:
```sql
INSERT INTO workspace.game_analytics.silver_events
VALUES ('BAD1', NULL, NULL, 'login', current_timestamp(), current_date(), NULL, 'manual', current_timestamp(), current_timestamp());
```
   Rerun `04_quality_checks`: it should **fail** on `no_null_keys`. Then remove it:
```sql
DELETE FROM workspace.game_analytics.silver_events WHERE event_id = 'BAD1';
```
   A pipeline that visibly stops on bad data is what separates you from most student projects.

---

## PHASE 5 - Gold (45 min)

1. Run `05_gold`.
2. Verify each table:
```sql
SELECT * FROM workspace.game_analytics.gold_daily_active_users ORDER BY event_date;
SELECT * FROM workspace.game_analytics.gold_retention ORDER BY cohort_date;
SELECT * FROM workspace.game_analytics.gold_top_games_by_genre ORDER BY genre, rank_in_genre;
SELECT * FROM workspace.game_analytics.gold_daily_revenue_by_game ORDER BY revenue DESC LIMIT 20;
```
**Understand:** `fact_events` + `dim_game` + `dim_player` is a **star schema**. The gold_* tables are pre-aggregated marts for BI.
Retention cohort = first day a player was seen; D7 is NULL for recent cohorts because the window has not completed. Explain that; it shows care.

---

## PHASE 6 - Orchestration (45-60 min)

1. Sidebar: **Jobs & Pipelines**, then **Create job**. Name: `game_analytics_daily`.
2. Add 4 tasks (type Notebook, compute Serverless), each depending on the previous:
   - `bronze` runs `02_bronze`
   - `silver` runs `03_silver` (depends on bronze)
   - `quality` runs `04_quality_checks` (depends on silver)
   - `gold` runs `05_gold` (depends on quality)
3. Set **Retries: 1** on each task, and add an **email notification on failure**.
4. Add a **Schedule**: daily at, say, 06:00.
5. Click **Run now**. Screenshot the successful DAG (the run graph).
6. If a job UI option is missing or unclear in your workspace, use `06_run_all` as a fallback and say so honestly in the README.

---

## PHASE 7 - Incremental load demo (20 min, high impact)

Simulates "a new day of data arrives":
1. Run `01_generate_data` with widgets: `start_date = 2026-09-15`, `num_days = 1`.
2. Run the job (or `06_run_all`).
3. Confirm: DAU table now has a 15th-Sept row; Bronze grew by only that one file; Silver rejects and dedupes correctly; earlier days did not change.
4. Screenshot the before/after row counts.

Now run the job a **second time with no new data**: nothing changes. That is idempotency end to end.

---

## PHASE 8 - Dashboard (45 min)

1. Sidebar: **Dashboards**, then **Create dashboard**. Use the SQL warehouse (2X-Small is fine).
2. Add datasets and visuals:

**DAU line chart**
```sql
SELECT event_date, dau FROM workspace.game_analytics.gold_daily_active_users ORDER BY event_date
```
**Revenue by genre bar chart**
```sql
SELECT genre, ROUND(SUM(revenue),2) AS revenue
FROM workspace.game_analytics.gold_daily_revenue_by_game GROUP BY genre ORDER BY revenue DESC
```
**Retention line chart** (x = cohort_date, y = d1_retention_pct and d7_retention_pct)
```sql
SELECT cohort_date, d1_retention_pct, d7_retention_pct
FROM workspace.game_analytics.gold_retention ORDER BY cohort_date
```
**Top 10 games table**
```sql
SELECT game_name, genre, ROUND(SUM(revenue),2) AS revenue
FROM workspace.game_analytics.gold_daily_revenue_by_game
GROUP BY game_name, genre ORDER BY revenue DESC LIMIT 10
```
**Data quality tile** (counter or table)
```sql
SELECT check_name, passed, detail FROM workspace.game_analytics.dq_results
WHERE run_ts = (SELECT MAX(run_ts) FROM workspace.game_analytics.dq_results)
```
3. Publish and screenshot the whole dashboard. (If you prefer Power BI, connect via Databricks connector; but the built-in dashboard is faster tonight.)

---

## PHASE 9 - README, repo polish, resume (60-75 min)

1. Create a `docs/` folder and put in: architecture diagram (draw.io, export PNG), job DAG screenshot, dashboard screenshot, DQ results screenshot, reject breakdown screenshot.
2. Open `README.md` here, replace every `<...>` placeholder, and drop in the screenshots.
3. Final commit and push. Pin the repo on your GitHub profile.
4. **Resume lines** (edit numbers to your actual counts):
   - *Built an end-to-end medallion lakehouse pipeline on Databricks (PySpark, Delta Lake, Auto Loader) processing ~700K simulated game events with incremental, idempotent loads via Delta MERGE.*
   - *Implemented automated data-quality gates (9 checks) and a quarantine table for rejected records, scheduled with Databricks Workflows including retries and failure alerts.*
   - *Modeled a star schema and business marts (DAU, cohort retention, revenue by genre) and published an AI/BI dashboard.*

---

## Architecture diagram (draw this)

```
 Python generator ──> Volume: events/*.json (daily, dirty)      games.csv, players.csv
                                  │                                    │
                          Auto Loader (incremental)              batch read
                                  ▼                                    ▼
 BRONZE   bronze_events (all strings + audit cols)       bronze_games, bronze_players
                                  │
                       parse / validate / dedupe / MERGE
                                  ▼
 SILVER   silver_events   silver_rejects (quarantine)    silver_games, silver_players
                                  │
                        QUALITY GATE (fails job on bad data) ──> dq_results
                                  ▼
 GOLD     fact_events, dim_game, dim_player
          gold_daily_active_users, gold_retention, gold_daily_revenue_by_game, gold_top_games_by_genre
                                  ▼
                  AI/BI Dashboard        (orchestrated by Databricks Workflow, daily)
```

---

## Interview prep: questions you WILL get

1. **Why medallion (Bronze/Silver/Gold)?** Separation of concerns: Bronze preserves raw data for replay, Silver holds trusted clean data, Gold serves business use cases. Bugs in cleaning never destroy source data.
2. **Why Delta instead of plain Parquet?** ACID transactions, MERGE/UPDATE/DELETE, schema enforcement, time travel, safe concurrent reads/writes.
3. **How did you make it idempotent?** MERGE on event_id (insert only if not matched) plus a watermark filter, so reruns and overlaps never duplicate rows. Gold is rebuilt with CREATE OR REPLACE.
4. **How do you handle bad data?** Parse with try_ functions, tag a reject_reason, quarantine into silver_rejects, and a quality gate stops the job if invariants break.
5. **How do you handle late-arriving data?** Late events have new event_ids, so they MERGE in; Gold is recomputed from Silver so aggregates include them.
6. **What is Auto Loader and why use it?** Incremental file discovery with checkpointed state; no manual bookkeeping of processed files; scales to millions of files.
7. **What would you change at 100x scale / real-time?** Replace file generator with Kafka/Event Hubs and Structured Streaming; partition or liquid-cluster Silver by event_date; incremental Gold instead of full rebuild; Unity Catalog governance and CI/CD via Databricks Asset Bundles.
8. **What would you monitor in production?** Row counts per layer, reject rate, freshness, job duration/failures. You already log some in `dq_results`.
9. **Why is `amount` a string in Bronze?** Source is untrusted; a strict type would drop or fail on bad values. Type at Silver where you control the handling.
10. **What went wrong and how did you fix it?** Have one real story: e.g. ANSI mode makes `to_timestamp` fail on bad strings, so you switched to `try_to_timestamp`. Real, and true.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `CATALOG workspace not found` | Catalog Explorer, find your catalog name, set it in `00_config` |
| `%run` cell error | The `%run` line must be alone in its cell: `# MAGIC %run ./00_config` |
| Permission error writing to `/Volumes/...` | Confirm the volume exists (`00_config` creates it); check catalog/schema names |
| Auto Loader "schema"/"checkpoint" errors after edits | Delete `/Volumes/workspace/game_analytics/raw/_checkpoints/bronze_events` and `DROP TABLE bronze_events`, then rerun 02 |
| Full reset (start over) | `DROP SCHEMA workspace.game_analytics CASCADE;` then rerun 00 and 01 |
| Notebook slow or "quota exceeded" | Free Edition has daily compute limits; lower `num_players` to 20000 in 01 and rerun |
| `MERGE` says table not found | Run `03_silver` from the top; the first run creates the table |
| SQL warehouse won't start | Free Edition has one 2X-Small warehouse; wait a minute and retry |

## If you run out of time: what to cut (in this order)
1. Dashboard extras (keep DAU and revenue only)
2. Incremental demo (keep the idempotency rerun)
3. Star-schema polish

**Never cut:** Silver, the quality gate, and the README. Those are what get you noticed.
