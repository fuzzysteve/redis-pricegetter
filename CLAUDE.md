# pricegetter-redis

SQL-free market loader: fetches EVE Online market orders via ESI and
publishes price aggregates straight to Redis — no Postgres, no SQL at
runtime. A flat JSON snapshot (`sde_snapshot.json`, built offline by
`build_sde_snapshot.py` from a SQLite copy of the EVE SDE) replaces the
database lookups a Postgres-backed version of this pipeline would do against
an `evesde`-style schema.

**This repo's working directory is the production deployment** — all three
`.sh` wrappers hardcode `cd /opt/pricegetter-redis`, and `config.cfg` here is
live deployment config, not a throwaway test fixture. Running any script
hits real ESI endpoints, can rotate the live OAuth refresh token, and writes
to the production Redis instance. Don't run `aggloader-esi.py`,
`structurePoller.py`, `checkFreshness.py`, or the `.sh` wrappers unless the
user explicitly asks you to.

## Runtime flow

- `loadPrices.sh` → `aggloader-esi.py` — main loader. Fetches all region
  market orders (concurrent, via `requests_futures`) plus authenticated
  structure-market orders for stations listed in `structures.json`, computes
  per-scope (region/station/solarSystem/universe) buy/sell aggregates,
  writes them to Redis with `ex=5400`. Meant to run frequently.
- `structurePoller.sh` → `structurePoller.py` — sweeps buy orders for
  `location_id > 1e8` plus ESI's public "structures with a market" listing,
  resolves each candidate to `{stationID, solarSystemID, regionID}` against
  `sde_snapshot.json`, writes `structures.json`. Meant to run far less often
  than the loader — structure discovery only needs to catch up with newly
  anchored/unanchored citadels.
- `build_sde_snapshot.py` — one-time/periodic offline tool, not part of the
  scheduled pipeline. Reads a SQLite copy of the SDE, writes
  `sde_snapshot.json` (regions, station→system→region map, every solar
  system's regionID). Only needs re-running when CCP adds/changes stations
  or systems.
- `checkFreshness.sh` → `checkFreshness.py` — independent canary. Checks
  Redis (not the loader's exit code) for the Tritanium@Jita-4-4 keys that
  `aggloader-esi.py` sets with `ex=5400` on every successful run. A missing
  key already means "not updated in the last 1.5h" via Redis's own TTL
  expiry — the check is key existence, not a timestamp comparison. Alerts
  via Discord webhook on staleness.

## Config

- `config.cfg` (read by `aggloader-esi.py`) / `structurePoller.cfg` (read by
  `structurePoller.py`) — both gitignored, INI format, hold OAuth client
  id/secret/refresh token, User-Agent, and worker count; `config.cfg`
  additionally holds the Redis connection and both scripts' data file paths.
  **Known to hold plaintext secrets — leave as-is unless the user explicitly
  asks to change it.** Never print their contents or echo them into
  logs/commits.
- Refresh tokens rotate: both scripts rewrite their config file in place via
  the local `atomic_write(path, write_fn)` helper (temp file in the same
  directory + `os.replace()`) whenever ESI returns a new `refresh_token`.
  Don't revert this to a plain `open(path, 'w')` — a crash mid-write would
  leave a truncated config file that the next run can't parse.
- `discord-webhook.txt` (gitignored) — Discord webhook URL for
  failure/staleness alerts, read by `lib-discord.sh`
  (`load_discord_webhook`/`notify_discord`, tracked, no secrets in it) and
  sourced by all three `.sh` wrappers. Also a secret — anyone with it can
  post to the channel — never print/log it or hardcode it into a tracked
  file.
- `sde_snapshot.json` / `structures.json` — gitignored, generated data, not
  source. Regenerate with `build_sde_snapshot.py` / `structurePoller.py`
  rather than hand-editing. A missing or empty `structures.json` is a valid
  state (no structures discovered yet, or `structurePoller.py` has simply
  never run) — `aggloader-esi.py` skips OAuth and structure-order fetching
  entirely in that case rather than erroring.

## Conventions worth knowing

- **Region list**: derived from `sde_snapshot.json`'s `regions` list (built
  from `staStations` at snapshot-build time), not hardcoded — except EVE's
  virtual PLEX Market region (`19000001`), which has no `staStations` row
  and is added explicitly in `aggloader-esi.py`'s `main()`.
- **Station vs. structure IDs**: IDs `>= 100000000` are player-owned
  structures, not real NPC stations. `structurePoller.py` uses this to spot
  candidate structure IDs in a region-wide buy-order sweep; `aggloader-esi.py`
  uses it to drop unauthenticated buy orders at citadels from the plain
  region fetch (those need the authenticated structure-order call instead).
- **ESI headers**: every request needs `User-Agent` and
  `X-Compatibility-Date` (`YYYY-MM-DD`), computed once per run as
  `utcnow() - 11h` (ESI's compatibility window flips at 11:00 UTC) — don't
  hardcode it or recompute it inconsistently within a run.
- **Redis key prefix**: `REDIS_KEY_PREFIX` in `aggloader-esi.py` and
  `checkFreshness.py` must stay in sync with each other — it's currently
  `""` (this deployment writes/reads the real production keys directly;
  there is no `cli-`/test-namespace guard anymore, that was removed once
  this became the production host). If you need to point a script at a
  shared/production Redis instance for testing without touching real keys,
  set a temporary prefix back in *both* files, not just one.
- **Key format**: `<scope>|<typeID>|<is_buy: "true"/"false">` (lowercase —
  `make_what()` in `aggloader-esi.py` maps Python bools to these strings
  explicitly, since plain `str(bool)` would produce `True`/`False` and break
  key matching against `checkFreshness.py` and other consumers). Value is
  pipe-delimited `weightedaverage|maxval|minval|stddev|median|volume|numorders|fivepercent`.
  Every key gets `ex=REDIS_TTL_SECONDS` (5400s/90min); keep that,
  `checkFreshness.py`'s 90-minute assumption, and `REDIS_KEY_PREFIX` in sync
  across both files if any of them change. `fp-lastupdate` holds the last
  fully-completed run's ISO timestamp.
- **Aggregation math** (`compute_side`/`run_scope_pass` in
  `aggloader-esi.py`) is vectorized — no `.apply(axis=1)` or
  `groupby(...).apply(lambda x: ...)`. Any group whose weight/`applies` sum
  is zero gets bumped to `0.01` *before* the division
  (`.mask(...transform('sum')==0, 0.01)`), which is why there's no
  `try/except ZeroDivisionError` here — fix issues the same way (bump the
  denominator upstream), don't reintroduce a per-group Python callback or a
  try/except.
- Avoid pandas' private/underscored methods (e.g. `_get_numeric_data()`)
  even as a shortcut — they can silently change behavior across pandas
  versions with no deprecation notice. Operate on the specific named column
  instead (see `compute_side`'s `.clip(lower=0)` on `applies`).
- **OAuth resilience**: `aggloader-esi.py` fetches region orders *before*
  refreshing the OAuth token needed for structure orders, and the
  refresh/structure-fetch block is wrapped in `try/except Exception` that
  logs and falls through. A refresh failure or structure-fetch error must
  never take down the whole run — region/station/system/universe aggregates
  should still get published from whatever was already fetched. Keep that
  ordering and the try/except if you touch this path.
- **ESI outage handling** (`aggloader-esi.py`; mirrors `/opt/pricegetter`):
  - **Preflight:** `main()` reads `GET https://esi.evetech.net/meta/status`,
    the API behind developers.eveonline.com/status. It returns per-route
    `OK`/`Degraded`/`Recovering`/`Down`.
  - **Skip the run:** if the status endpoint is unreachable, or
    `/markets/{region_id}/orders` is `Down`, nothing is fetched or
    published. "Unreachable" covers a connection error, a timeout, a
    non-2xx response, or unparseable JSON, and it is treated as "ESI is
    down".
  - **Structures Down:** if `/markets/structures/{structure_id}` is `Down`,
    OAuth and the structure fetch are skipped and the run carries on
    without them.
  - **Region data is all-or-nothing:** if any region page still fails after
    3 tries, if the 20-min `FETCH_DEADLINE_SECONDS` passes, or if zero
    orders were fetched, the run exits before writing anything to Redis. A
    partial region set silently skews the universe aggregates. Structure
    failures don't abort the run, which keeps the OAuth-resilience rule
    above: an `ESIUnhealthy` raised in the structure wave is caught by
    that `try/except`.
  - **Exit code and reporting:** all of these go through
    `esi_unhealthy_exit()`. It prints an `ESI-UNHEALTHY: <reason>` line to
    stderr and exits with code 75. `loadPrices.sh` posts that reason to
    Discord only for the first failure; it creates `esi-unhealthy.state`
    (gitignored) to remember the outage. It posts once more on the next
    successful run, when it also deletes the state file. The wrapper exits
    0 either way. Other failures still alert on every run through the
    `ERR` trap.
  - **Error limit:** 420/429 responses don't use up retries. The pause is
    applied once per batch from `X-ESI-Error-Limit-Reset`/`Retry-After`,
    never as a per-response `sleep`. Once
    `X-ESI-Error-Limit-Remain <= ERROR_LIMIT_FLOOR`, the rest of the batch
    is cancelled and requeued.
  - **Backoff:** retry rounds back off 5s, 10s, and so on (capped at 30s).
- All requests use `REQUEST_TIMEOUT = 30`; no blocking call should be added
  without a timeout.
- **Logging**: each Python script and each `.sh` wrapper write datestamped
  logs (`logs/<script>-YYYY-MM-DD.log`) and self-prune anything older than
  `LOG_RETENTION_DAYS` (7) — no logrotate dependency. Credentials/tokens are
  only ever logged at `--debug` level, never at default `INFO`.
- **Shell wrappers** (`loadPrices.sh`, `structurePoller.sh`,
  `checkFreshness.sh`) each take an `flock` on their own lock file (single
  instance only), rotate their own log, and run under `set -e -o pipefail`
  with an `ERR` trap that posts to Discord via `notify_discord` and stops
  immediately. `checkFreshness.sh`'s trap is for its own script-level
  errors — distinct from the staleness alert its Python script posts on a
  successful run that finds stale data.
- Uses `uv` (see `pyproject.toml`/`uv.lock`) — run scripts as `uv run python
  <script>.py`, not a bare venv `python`.

## Do not

- Don't run `aggloader-esi.py`, `structurePoller.py`, `checkFreshness.py`,
  or any `.sh` wrapper without the user explicitly asking — they act on
  production ESI credentials, Redis, and Discord.
- Don't hand-edit `sde_snapshot.json` or `structures.json` — regenerate them
  with `build_sde_snapshot.py` / `structurePoller.py`.
- Don't print, log, or commit the contents of `config.cfg`,
  `structurePoller.cfg`, or `discord-webhook.txt`.
- Don't reintroduce a hardcoded region list — derive it from
  `sde_snapshot.json`.
- Don't revert the `atomic_write()` calls back to plain `open(path, 'w')`
  for config or `structures.json` writes.
