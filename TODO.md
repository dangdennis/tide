# tide TODO

## Phase 1 — Redis client & Engine (DONE — compiles, untested)
- [ ] Integration tests against real Redis (docker-compose up)
- [ ] Verify Lua scripts work end-to-end (move_to_stream, unique_insert)
- [ ] Fix `now_ms()` stub — needs real clock (no stdlib time API found yet)
- [ ] Fix `generate_job_id()` stub — needs UUID or random hex
- [ ] Fix `generate_node_id()` stub — needs hostname + PID
- [ ] `prune_jobs` is unimplemented (stub returns 0)

## Phase 2 — Core runtime (NEXT)
Files to create:
- `src/worker/worker.mbt` — `Worker` trait: `perform`, `backoff`, `timeout`. `PerformResult` variants: `Ok | Snooze(seconds) | Discard(reason) | Error(e)`. Default backoff: `min(attempt^4 + 15, 86400)` seconds.
- `src/queue/queue.mbt` — async loop per queue: drain available→stream via Lua, then `XREADGROUP BLOCK` to claim jobs, spawn one executor task per job.
- `src/queue/executor.mbt` — runs one job inside its timeout. On result: Ok→complete, Error+attempts→retry with backoff, Error+exhausted→discard, Snooze→snooze, Discard→discard.
- `src/queue/shutdown.mbt` — graceful drain: stop fetching, wait up to `shutdown_grace` (default 15s) for in-flight jobs, force-cancel remaining.
- `src/tide/instance.mbt` — wire up `Tide.start` to spawn queue loops; `Tide.stop` for graceful shutdown.

Exit criteria: insert job → queue loop fetches → executor calls `perform` → job hash → `completed`. Kill worker mid-job → PEL reclaim on restart.

## Phase 3 — Scheduling & maintenance plugins
- `src/plugin/stager.mbt` — every 1s: move scheduled→available for jobs whose score ≤ now
- `src/plugin/cron.mbt` — per-minute cron tick; idempotent unique-key insert
- `src/plugin/cron/parser.mbt` — 5-field cron + `@hourly`/`@daily`/etc.
- `src/plugin/pruner.mbt` — delete terminal-state jobs older than `max_age`
- `src/plugin/lifeline.mbt` — `XAUTOCLAIM` to reclaim PEL entries idle > `rescue_after` (30s)

## Phase 4 — Job features
- `src/job/unique.mbt` — unique jobs via `unique_insert.lua`; period + fields + states
- `src/job/priority.mbt` — 10 levels (0–9); score = priority * 1e12 + scheduled_at_ms
- `src/job/tags.mbt` — tag-based cancel/retry filtering
- `src/job/meta.mbt` — arbitrary JSON metadata field

## Phase 5 — Multi-node coordination
- `src/peer/redis.mbt` — leader election: `SET tide:peer:{name} {node_id} NX PX 30000`, renewed every 15s
- `src/peer/local.mbt` — single-node stub (always leader)
- `src/notifier/redis.mbt` — optional pub/sub for cancel signals and concurrency scaling

## Phase 6 — Transactional outbox
- `src/outbox/postgres.mbt` — `INSERT INTO tide_outbox` within caller's transaction
- `src/outbox/sqlite.mbt` — same for SQLite
- `src/outbox/relay.mbt` — poll outbox, call `Tide.insert`, delete row; `SELECT FOR UPDATE SKIP LOCKED`
- `src/migration/postgres.mbt` — DDL for `tide_outbox`
- `src/migration/sqlite.mbt` — DDL for `tide_outbox`

## Phase 7 — Telemetry & testing helpers
- `src/telemetry/event.mbt` — `[:tide, :job, :start | :stop | :exception]` events; `Handler` trait
- `src/testing/helpers.mbt` — `assert_enqueued`, `drain_queue`, `perform_inline`
- `src/testing/sandbox.mbt` — in-memory engine for unit tests (no Redis)

## Phase 8 — Post-1.0
- Workflows (DAG of jobs)
- Batches (group + on_complete callbacks)
- Rate limiting (token bucket via Lua)
- Global concurrency (Redis-coordinated counter)
- Encrypted args (AES-GCM)
