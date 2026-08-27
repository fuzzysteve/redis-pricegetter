# pricegetter-redis

EVE Online market loader that fetches ESI market orders and publishes price
aggregates straight to Redis — no SQL database required. A flat JSON
snapshot of the relevant EVE Static Data Export (SDE) tables stands in for
the database lookups a Postgres-backed version of this pipeline would do.

## What it does

- Fetches every region's public market orders from ESI, plus authenticated
  market orders for known player-owned structures (citadels).
- Computes buy/sell price aggregates at four scopes — region, station, solar
  system, and universe-wide — for every `(scope, typeID, buy/sell)`
  combination: weighted average, min, max, median, standard deviation, total
  volume, order count, and a 5%-of-volume depth-weighted price.
- Publishes each aggregate to Redis with a 90-minute TTL, so a missing key
  already means "hasn't been refreshed recently".
- Separately discovers citadels with a public market (`structurePoller.py`)
  and checks Redis for staleness with a Discord alert on failure
  (`checkFreshness.py`).

## Components

| File | Purpose |
|---|---|
| `aggloader-esi.py` | Main loader: fetches orders, computes aggregates, writes to Redis. Run frequently (e.g. every 30 min). |
| `structurePoller.py` | Discovers citadels with a public market, writes `structures.json`. Run infrequently (e.g. daily). |
| `build_sde_snapshot.py` | One-time/periodic offline tool: converts a SQLite copy of the EVE SDE into `sde_snapshot.json`. Only needs re-running when CCP adds/changes stations or systems. |
| `checkFreshness.py` | Canary: checks Redis for a known liquid item/station pair (Tritanium at Jita 4-4) and alerts via Discord if stale. |
| `loadPrices.sh` / `structurePoller.sh` / `checkFreshness.sh` | Cron/systemd-timer wrappers — locking, log rotation, and Discord alerting around the scripts above. |
| `lib-discord.sh` | Shared `load_discord_webhook` / `notify_discord` helpers, sourced by the three wrapper scripts. |

## Setup

1. Install dependencies: `uv sync`
2. Build the SDE snapshot once, and again whenever CCP changes stations or
   systems:
   ```
   uv run python build_sde_snapshot.py /path/to/sde.sqlite
   ```
3. Create `config.cfg` in the repo root (gitignored — see format below) with
   your ESI application's OAuth credentials and your Redis connection.
4. If you want structure-order discovery, also create `structurePoller.cfg`
   in the same format.
5. Optionally create `discord-webhook.txt` (a single line: your Discord
   webhook URL) for failure/staleness alerts.
6. Schedule the wrapper scripts: `loadPrices.sh` frequently (e.g. every
   30 min), `structurePoller.sh` infrequently (e.g. daily),
   `checkFreshness.sh` frequently, offset a few minutes after
   `loadPrices.sh` so it checks after a load has had a chance to finish.

## Configuration

`config.cfg` / `structurePoller.cfg` (INI format):

```ini
[oauth]
clientid = your-esi-app-client-id
secret = your-esi-app-secret
refreshtoken = a-refresh-token-with-structure-market-scope

[requests]
useragent = your-app-name (you@example.com)
max_workers = 10

[redis]
host = localhost
port = 6379
db = 0
; password = optional

[data]
sde_snapshot = sde_snapshot.json
structures = structures.json
```

`refreshtoken` rotates: both scripts rewrite it in place (atomically) in
their own config file whenever ESI returns a new one.

`structurePoller.cfg` additionally reads `[data] out` for where to write
`structures.json` (defaults to `structures.json`), and doesn't need a
`[redis]` section — it never touches Redis.

## Redis key format

```
<scope>|<typeID>|<true|false>
```

- `scope` is a `regionID`, `stationID`, `solarSystemID`, or the literal `0`
  for the universe-wide aggregate.
- `true`/`false` is buy/sell.

Value is a pipe-delimited string:

```
weightedaverage|maxval|minval|stddev|median|volume|numorders|fivepercent
```

Every key is set with a 5400-second (90-minute) TTL. A separate
`fp-lastupdate` key holds the ISO-8601 timestamp of the last fully-completed
`aggloader-esi.py` run.

## Running manually

```
uv run python build_sde_snapshot.py /path/to/sde.sqlite
uv run python structurePoller.py
uv run python aggloader-esi.py
uv run python checkFreshness.py
```

Or via the wrapper scripts, which add locking (so a slow run can't overlap
itself), log rotation, and Discord alerting on failure:

```
./loadPrices.sh
./structurePoller.sh
./checkFreshness.sh
```

Both `aggloader-esi.py` and `structurePoller.py` accept `--regions
<comma-separated-regionIDs>` to limit a run to specific regions (testing
only), and `--debug` for verbose logging.

## Requirements

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)
- A reachable Redis instance
- An EVE ESI application (client id/secret) and a refresh token with
  structure-market scope, if you want structure-order support
