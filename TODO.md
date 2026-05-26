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

## Phase 4 — Job features ✅ COMPLETE
- [x] `src/job/unique.mbt` — `UniqueConfig` struct + `compute_unique_key`; engine: atomic `insert_unique_job` via `unique_insert.lua`; `Instance::insert_unique`
- [x] `src/job/priority.mbt` — `PRIORITY_*` constants (0–9); `priority_valid`; `with_priority_clamped`
- [x] `src/job/tags.mbt` — `job_has_tag` + `jobs_with_tag`; engine: `cancel_jobs_by_tag` + `retry_jobs_by_tag`; `Instance::cancel_by_tag` + `retry_by_tag`
- [x] `src/job/meta.mbt` — `meta_get_string/Bool` + `meta_set_string/Bool` helpers for the flat-JSON meta field

Exit criteria met: unique jobs atomically deduplicated via Lua script; named priority constants with clamping; tag-based bulk cancel/retry through Redis SCAN; flat-JSON meta helpers for arbitrary metadata.

## Phase 5 — Multi-node coordination ✅ COMPLETE
- [x] `src/peer/redis.mbt` — leader election: `SET tide:peer:{name} {node_id} NX PX 30000`, renewed every 15s
- [x] `src/peer/local.mbt` — single-node stub (always leader)
- [x] `src/notifier/redis.mbt` — optional pub/sub for cancel signals and concurrency scaling
- [x] `src/tide/tide.mbt` — `PeerMode` enum; `Config.peer_mode` + `Config.notify_cancel`; `Instance.peer` + `Instance.notifier`; peer renewal loop + notifier listener spawned in `run()`; cancel check before job dispatch; `Instance::is_leader()`

Exit criteria met: single-node mode requires no config change (Local peer, notify_cancel=false); Redis mode runs election every 15 s with 30 s TTL; cancel signals propagate via pub/sub to all nodes; pre-dispatch cancel check prevents executing already-cancelled jobs.

## Phase 6 — Transactional outbox ✅
- `src/outbox/db.mbt` — `DbConn` open trait, `DbValue` enum, `Row`/`OutboxRow` structs, `OutboxError`
- `src/outbox/outbox.mbt` — `insert[C: DbConn]()` (Postgres) and `insert_sqlite[C: DbConn]()` caller API
- `src/outbox/postgres.mbt` — `PgClientConn` and `PgTxConn` adapters; `PG_CLAIM_SQL`, `PG_DELETE_SQL`
- `src/outbox/sqlite.mbt` — `SqliteConn` adapter; `SQLITE_CLAIM_SQL`, `SQLITE_DELETE_SQL`
- `src/outbox/relay.mbt` — `Relay[C]` polling loop; `parse_outbox_row`, `outbox_row_to_job`
- `src/migration/postgres.mbt` — `POSTGRES_DDL` (CREATE TABLE + INDEX)
- `src/migration/sqlite.mbt` — `SQLITE_DDL` (CREATE TABLE + INDEX)
- `src/outbox/outbox_wbtest.mbt` — 5 unit tests using in-memory SQLite (no external service required)

## Phase 7 — Telemetry & testing helpers ✅ COMPLETE
- [x] `src/telemetry/event.mbt` — `TelemetryEvent` enum (JobStart/JobStop/JobException) + `JobEventPayload` + `Handler` open trait + payload builder helpers
- [x] `src/testing/sandbox.mbt` — `SandboxEngine` implementing the full `Engine` trait with in-memory Map storage; inspection helpers (`jobs_in_state`, `jobs_for_queue`, `job_count`)
- [x] `src/testing/helpers.mbt` — `perform_inline`, `assert_enqueued`, `assert_enqueued_where`, `drain_queue`
- [x] 15 whitebox tests for sandbox + helpers pass (total: 226 tests, all pass)

Exit criteria met: workers can be unit-tested without Redis using SandboxEngine; `perform_inline` calls a worker directly; `drain_queue` runs all available jobs; `assert_enqueued` checks queue contents; telemetry Handler trait ready to be wired into the executor.

## Phase 8 — Dashboard ✅ COMPLETE
- [x] `src/dashboard/api.mbt` — Redis query layer: `queue_stats`, `list_jobs` (available/scheduled/executing/terminal), `get_job`, `retry_job`, `cancel_job`, `delete_job`
- [x] `src/dashboard/handler.mbt` — HTTP routing via `@http.Server`; JSON serialization; URL/query-string parsing; `serve(engine, queues, port)` entry point
- [x] `src/dashboard/html.mbt` — Embedded single-page app (`DASHBOARD_HTML` const); vanilla HTML/CSS/JS; queue tabs, stats bar, job table, detail panel, retry/cancel/delete actions, auto-refresh every 5s
- [x] `src/dashboard/moon.pkg` — imports async/http, async/socket, redis, engine, job
- [x] `src/tide/tide.mbt` — `Config.dashboard_port : Int?` (default `None`); spawns `@dashboard.serve()` as background task in `Instance::run()` when set
- [x] All existing 226 tests continue to pass (37 Redis integration tests require docker-compose)

Usage: set `dashboard_port: Some(4567)` in `Config` and open `http://localhost:4567`.

## Phase 9 — Post-1.0
- Workflows (DAG of jobs)
- Batches (group + on_complete callbacks)
- Rate limiting (token bucket via Lua)
- Global concurrency (Redis-coordinated counter)
- Encrypted args (AES-GCM)
