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

## Phase 8 - Dashboard

## Phase 9 — Post-1.0
- Workflows (DAG of jobs)
- Batches (group + on_complete callbacks)
- Rate limiting (token bucket via Lua)
- Global concurrency (Redis-coordinated counter)
- Encrypted args (AES-GCM)
