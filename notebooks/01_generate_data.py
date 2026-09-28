# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Generate raw game data (simulated source systems)
# MAGIC Writes to the Unity Catalog Volume:
# MAGIC - `events/events_<date>.json` - one JSON-lines file per day (deliberately dirty)
# MAGIC - `games/games.csv`, `players/players.csv` - reference data
# MAGIC
# MAGIC Dirty data injected on purpose: duplicates, null player_id, bad/misspelled event types,
# MAGIC mixed timestamp formats, invalid purchase amounts, late-arriving events.
# MAGIC
# MAGIC Run once with defaults (14 days). To demo incremental loads later, run again with
# MAGIC start_date=2026-09-15, num_days=1.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("start_date", "2026-09-01", "First day to generate (YYYY-MM-DD)")
dbutils.widgets.text("num_days", "14", "Number of days to generate")
dbutils.widgets.text("num_players", "60000", "Player universe size")

# COMMAND ----------

import csv
import json
import os
import random
from datetime import date, datetime, timedelta

START = date.fromisoformat(dbutils.widgets.get("start_date"))
NUM_DAYS = int(dbutils.widgets.get("num_days"))
NUM_PLAYERS = int(dbutils.widgets.get("num_players"))

UNIVERSE_START = date(2026, 9, 1)   # players sign up relative to this date
SIGNUP_WINDOW_DAYS = 28

for sub in ["events", "games", "players"]:
    os.makedirs(f"{BASE}/{sub}", exist_ok=True)

# COMMAND ----------
# MAGIC %md ## Reference data: games and players (deterministic, so reruns are identical)

# COMMAND ----------

rng = random.Random(42)

GENRES = ["RPG", "FPS", "Puzzle", "Strategy", "Racing", "Sports", "Fighting", "Platformer"]
ADJ = ["Shadow", "Crimson", "Neon", "Iron", "Mystic", "Turbo", "Pixel", "Storm", "Lunar", "Ember"]
NOUN = ["Quest", "Arena", "Legends", "Rush", "Tactics", "Odyssey", "Clash", "Realms"]
combos = [f"{a} {n}" for a in ADJ for n in NOUN]
rng.shuffle(combos)

games = [
    {
        "game_id": f"G{i:03d}",
        "game_name": name,
        "genre": rng.choice(GENRES),
        "base_price": rng.choice([0, 0, 4.99, 9.99, 19.99, 29.99]),
    }
    for i, name in enumerate(combos[:40], 1)
]

COUNTRIES = ["India", "Japan", "USA", "Brazil", "Germany", "UK", "Indonesia", "Mexico"]
PLATFORMS = ["PC", "PlayStation", "Xbox", "Switch", "Mobile"]
players = [
    {
        "player_id": f"P{i:06d}",
        "signup_date": UNIVERSE_START + timedelta(days=rng.randrange(SIGNUP_WINDOW_DAYS)),
        "country": rng.choice(COUNTRIES),
        "platform": rng.choice(PLATFORMS),
    }
    for i in range(1, NUM_PLAYERS + 1)
]

with open(f"{BASE}/games/games.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["game_id", "game_name", "genre", "base_price"])
    w.writeheader()
    w.writerows(games)

with open(f"{BASE}/players/players.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["player_id", "signup_date", "country", "platform"])
    w.writeheader()
    for p in players:
        w.writerow({**p, "signup_date": p["signup_date"].isoformat()})

print(f"Wrote {len(games)} games and {len(players)} players")

# COMMAND ----------
# MAGIC %md ## Event generator

# COMMAND ----------

FMT_A = "%Y-%m-%d %H:%M:%S"
FMT_B = "%d/%m/%Y %H:%M:%S"
PURCHASE_AMOUNTS = [0.99, 1.99, 4.99, 9.99, 19.99]


def generate_day(day):
    r = random.Random(day.toordinal())   # deterministic per day
    rows = []
    counter = 0

    def emit(pid, gid, etype, ts, amount=None):
        nonlocal counter
        counter += 1
        rows.append({
            "event_id": f"E{day:%Y%m%d}{counter:07d}",
            "player_id": pid,
            "game_id": gid,
            "event_type": etype,
            "ts": ts,
            "amount": amount,
        })

    day_dt = datetime.combine(day, datetime.min.time())

    # 1) clean sessions. Activity probability decays with days since signup -> realistic retention
    for p in players:
        age = (day - p["signup_date"]).days
        if age < 0:
            continue
        if r.random() > 0.12 + 0.75 * (0.82 ** age):
            continue
        t = day_dt + timedelta(hours=r.randint(8, 20), minutes=r.randint(0, 59), seconds=r.randint(0, 59))
        pid = p["player_id"]
        emit(pid, None, "login", t)
        for _ in range(r.randint(1, 4)):
            g = r.choice(games)["game_id"]
            t += timedelta(minutes=r.randint(1, 5))
            emit(pid, g, "match_start", t)
            t += timedelta(minutes=r.randint(5, 25))
            emit(pid, g, "match_end", t)
            if r.random() < 0.06:
                t += timedelta(minutes=1)
                emit(pid, g, "purchase", t, r.choice(PURCHASE_AMOUNTS))
        t += timedelta(minutes=1)
        emit(pid, None, "logout", t)

    # 2) make it dirty
    out = []
    for row in rows:
        ts = row.pop("ts")
        if day > UNIVERSE_START and r.random() < 0.02:
            ts -= timedelta(days=1)                       # late-arriving event
        row["event_time"] = ts.strftime(FMT_B if r.random() < 0.03 else FMT_A)   # mixed formats

        x = r.random()
        if x < 0.01:
            row["player_id"] = None                       # missing key
        elif x < 0.02:
            if r.random() < 0.5:
                row["event_type"] = row["event_type"].upper() + " "     # fixable in Silver
            else:
                row["event_type"] = r.choice(["logn", "match_strt", "purchse", "unknown"])  # rejected

        if row["amount"] is not None:
            y = r.random()
            if y < 0.02:
                row["amount"] = "N/A"
            elif y < 0.03:
                row["amount"] = -abs(row["amount"])
            elif y < 0.08:
                row["amount"] = str(row["amount"])        # number stored as string
        out.append(row)

    # 3) duplicates (2%)
    out.extend(dict(d) for d in r.sample(out, int(len(out) * 0.02)))
    r.shuffle(out)
    return out


# COMMAND ----------

total = 0
for i in range(NUM_DAYS):
    day = START + timedelta(days=i)
    rows = generate_day(day)
    path = f"{BASE}/events/events_{day.isoformat()}.json"
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    total += len(rows)
    print(f"{day}: {len(rows):>7,} events -> {path}")

print(f"\nDone. {total:,} events written.")
