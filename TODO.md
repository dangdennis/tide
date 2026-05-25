# tide TODO

## Phase 1 — Redis client & Engine ✅ COMPLETE
- [x] Integration tests against real Redis (docker-compose up) — 16 engine tests pass
- [x] Verify Lua scripts work end-to-end (move_to_stream, unique_insert) — confirmed via e2e tests
- [x] Fix `now_ms()` stub — uses `@async.now()`
- [x] Fix `generate_job_id()` stub — counter + timestamp
- [x] Fix `generate_node_id()` stub — startup-time-based
- [x] Fix `parse_json_string_array()` stub — hand-rolled parser
- [x] `prune_jobs` — implemented via SCAN + HGETALL + DEL; `Connection::scan` added to client
- [x] Fix `recv_response` hang — `bytes.to_unchecked_string()` garbled UTF-16; replaced with `@utf8.decode_lossy`
- [x] Fix RESP bulk string byte count — use `@utf8.encode(arg).length()` not `arg.length()`
- [x] Validate AUTH/SELECT responses — `SimpleErr` now raises `ConnectionFailed`
- [x] Persist error messages — JSON entry written to job `errors` array on retry/discard
- [x] Set `attempted_at` and `attempted_by` — written to hash on fetch

## Phase 2 — Core runtime ✅ COMPLETE
- [x] `src/worker/worker.mbt` — `PerformResult` enum + `WorkerFn` type alias
- [x] `src/queue/queue.mbt` — `QueueRunner` struct + `fetch_batch()`
- [x] `src/queue/executor.mbt` — `execute()`: timeout, retry/discard/snooze logic
- [x] `src/queue/shutdown.mbt` — `ShutdownSignal` via `CondVar`
- [x] `src/tide/tide.mbt` — `Instance::run()` spawns queue loops via `with_task_group`; `Instance::stop()` triggers graceful drain; `Instance::register()` for worker functions
- [x] End-to-end integration tests pass (insert → fetch → perform → completed)

Exit criteria met: insert job → queue loop fetches → executor calls `perform` → job hash → `completed`.
Note: PEL reclaim on crash-restart is Phase 3 (Lifeline plugin).

## Phase 3 — Scheduling & maintenance plugins ✅ COMPLETE
- [x] `src/plugin/stager.mbt` — every 1s: move scheduled→available for jobs whose score ≤ now
- [x] `src/plugin/cron.mbt` — per-minute cron tick; idempotent unique-key insert (SET NX PX)
- [x] `src/plugin/cron/parser.mbt` — 5-field cron + `@hourly`/`@daily`/etc. + unit tests
- [x] `src/plugin/pruner.mbt` — delete terminal-state jobs older than `max_age`
- [x] `src/plugin/lifeline.mbt` — `XAUTOCLAIM` to reclaim PEL entries idle > `rescue_after` (30s)
- [x] Wire all plugins into `Instance::run` via spawn_loop; controlled by `Config` fields
- [x] `Config` additions: `prune_max_age_ms`, `lifeline_rescue_after_ms`, `cron_entries`

Exit criteria met: stager promotes scheduled jobs every 1 s; lifeline reclaims crashed-worker PEL
entries every 30 s; pruner deletes old terminal jobs (opt-in via prune_max_age_ms); cron fires
idempotent per-minute jobs with cross-node deduplication via SET NX PX.

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
