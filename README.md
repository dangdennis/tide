# tide

A Redis-backed background job queue for MoonBit, inspired by [Oban](https://github.com/oban-bg/oban) (Elixir) and [Sidekiq](https://github.com/sidekiq/sidekiq) (Ruby).

- **Reliable delivery** — jobs survive crashes via Redis Streams + PEL reclaim
- **Scheduling** — run jobs immediately, at a future time, or on a cron schedule
- **Concurrency control** — per-queue semaphore limits simultaneous execution
- **Unique jobs** — atomic deduplication across nodes with a configurable time window
- **Multi-node** — leader election and cancel-signal pub/sub out of the box
- **Transactional outbox** — insert jobs atomically inside your existing DB transaction
- **Dashboard** — embedded web UI; one config field to enable

## Quick start

```bash
moon add dangdennis/tide
```

```moonbit
// 1. Create a config
let config = {
  ..@tide.Config::default(),
  redis_url: "redis://127.0.0.1:6379",
  queues: Map([("default", 10)]),   // queue name → max concurrency
}

// 2. Register workers
let instance = @tide.Instance::start(config)
instance.register("SendEmailWorker", async fn(job) {
  println("sending email: \{job.args}")
  @worker.PerformResult::Ok
})

// 3. Enqueue a job (anywhere in your app)
let job = @job.Job::new("default", "SendEmailWorker")
  .with_args("{\"to\":\"user@example.com\"}")
instance.insert(job)

// 4. Start processing (blocks until instance.stop() is called)
instance.run()
```

## Workers

A worker is any `async (@job.Job) -> @worker.PerformResult raise Error` function. Return one of:

| Result | Effect |
|--------|--------|
| `Ok` | Job marked completed |
| `Snooze(seconds)` | Job rescheduled by N seconds (attempt count unchanged) |
| `Discard(reason)` | Job marked discarded immediately, regardless of remaining attempts |

If the function raises an error or times out, the job is retried with exponential backoff until `max_attempts` is exhausted.

```moonbit
instance.register("ResizeImageWorker", async fn(job) {
  let id = parse_args(job.args)
  if !file_exists(id) {
    return @worker.PerformResult::Discard("file not found")
  }
  resize(id)
  @worker.PerformResult::Ok
})
```

## Job options

```moonbit
let job = @job.Job::new("emails", "WelcomeEmailWorker")
  .with_args("{\"user_id\":42}")           // JSON payload
  .with_scheduled_at(now_ms() + 60000L)   // run in 60 seconds
  .with_max_attempts(5)                   // default: 3
  .with_priority(@job.PRIORITY_HIGH)      // 0 (highest) – 9 (lowest), default 5
  .with_tags(["onboarding", "email"])     // for bulk operations
```

## Unique jobs

Prevent duplicate jobs within a time window. The uniqueness key is derived from `queue + worker + args` by default.

```moonbit
let job = @job.Job::new("default", "DailyReportWorker")

instance.insert_unique(job, @job.UniqueConfig::{
  period_ms: 3600000L,   // 1 hour dedup window
  by_args: true,
  by_queue: true,
})
// Returns Inserted(job) or AlreadyExists
```

## Scheduled jobs

```moonbit
// Run at a specific time
let job = @job.Job::new("default", "SendReminderWorker")
  .with_scheduled_at(unix_ms_at_9am_tomorrow())
instance.insert(job)
```

## Cron jobs

Cross-node deduplicated — fires at most once per minute across your whole fleet.

```moonbit
let config = {
  ..@tide.Config::default(),
  cron_entries: [
    @plugin.CronEntry::{
      schedule: "@daily",
      queue: "maintenance",
      worker: "CleanupWorker",
      args: "{}",
    },
    @plugin.CronEntry::{
      schedule: "0 9 * * 1-5",   // 09:00 Mon–Fri
      queue: "reports",
      worker: "WeekdayReportWorker",
      args: "{}",
    },
  ],
}
```

Supported shorthands: `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`.

## Tag-based operations

```moonbit
// Cancel all non-terminal jobs with tag "user:42"
instance.cancel_by_tag("default", "user:42")

// Re-enqueue discarded/cancelled jobs with that tag
instance.retry_by_tag("default", "user:42")
```

## Multi-node coordination

For multi-process or multi-host deployments, enable Redis-backed leader election and cancel propagation:

```moonbit
let config = {
  ..@tide.Config::default(),
  name: "my-app",          // unique per logical cluster
  peer_mode: Redis,        // leader election via SET NX PX
  notify_cancel: true,     // broadcast cancel signals via pub/sub
}

// Check if this node is currently the leader
instance.is_leader()
```

Each node holds a 30-second lease renewed every 15 seconds. Cancel signals propagate via `tide:notify:{name}` pub/sub so all nodes skip already-cancelled jobs.

## Graceful shutdown

```moonbit
// Signal all queue loops to stop accepting new jobs
instance.stop()
// Instance::run() returns once all in-flight jobs finish
// (or after shutdown_grace_ms, default 15 s)
```

## Maintenance plugins

All enabled via `Config`:

| Config field | Default | Description |
|---|---|---|
| `lifeline_rescue_after_ms` | 30 000 | Reclaim PEL entries idle longer than this (crash recovery) |
| `prune_max_age_ms` | `None` | Delete terminal jobs older than N ms (disabled by default) |
| `cron_entries` | `[]` | Scheduled job definitions |

```moonbit
let config = {
  ..@tide.Config::default(),
  prune_max_age_ms: Some(604800000L),  // prune jobs older than 7 days
  lifeline_rescue_after_ms: 60000L,   // reclaim after 60 s idle
}
```

## Transactional outbox

Solve the dual-write problem: enqueue a job inside your existing database transaction so it's never lost or double-delivered if the transaction rolls back.

### 1. Apply the schema migration

```moonbit
// Postgres
pg_conn.execute(@migration.POSTGRES_DDL)

// SQLite
sqlite_conn.execute(@migration.SQLITE_DDL)
```

### 2. Insert inside your transaction

```moonbit
// Postgres — inside a BEGIN / COMMIT block
let job = @job.Job::new("billing", "ChargeWorker").with_args(args)
@outbox.insert(@outbox.PgTxConn::new(tx), job)
// commit your business logic + the outbox row atomically
```

```moonbit
// SQLite
@outbox.insert_sqlite(@outbox.SqliteConn::new(conn), job)
```

### 3. Start the relay alongside your tide instance

```moonbit
let relay = @outbox.Relay::new(
  instance=tide_instance,
  conn=@outbox.PgClientConn::new(relay_client),
  claim_sql=@outbox.PG_CLAIM_SQL,
  delete_sql=@outbox.PG_DELETE_SQL,
)
@async.with_task_group(async fn(tg) {
  tg.spawn_bg(allow_failure=true) <| () => { tide_instance.run() catch { _ => () } }
  tg.spawn_bg(allow_failure=true) <| () => { relay.run()          catch { _ => () } }
})
```

The relay polls `tide_outbox` every 500 ms, forwards each row to tide (Redis), and deletes it. At-least-once delivery; use unique jobs for deduplication.

## Telemetry

Implement `@telemetry.Handler` to observe job lifecycle events:

```moonbit
pub struct MyHandler {}

pub impl @telemetry.Handler for MyHandler with handle(self, event) {
  match event {
    JobStart(p)     => log("start", p.job.id, p.started_at)
    JobStop(p)      => log("done",  p.job.id, p.duration_ms)
    JobException(p) => log("error", p.job.id, p.error)
  }
}
```

Events carry a `JobEventPayload` with the job, `started_at` (epoch ms), `duration_ms`, and an `error` string (non-empty only for `JobException`).

## Testing

Test workers without Redis using `SandboxEngine`:

```moonbit
let eng = @testing.SandboxEngine::new()

// Register workers
let workers = Map([
  ("MyWorker", async fn(job) { @worker.PerformResult::Ok }),
])

// Insert and verify
let _ = eng.insert_job(@job.Job::new("default", "MyWorker"))
assert_eq(@testing.assert_enqueued(eng, "default", "MyWorker"), true)

// Run all available jobs to completion
let count = @testing.drain_queue(eng, workers, "default")
assert_eq(count, 1)
assert_eq(eng.jobs_in_state(@job.Completed).length(), 1)
```

**`perform_inline`** — call a worker directly, bypassing the engine entirely:

```moonbit
let result = @testing.perform_inline(workers, job)
// returns PerformResult without touching any state
```

**`assert_enqueued_where`** — check by predicate:

```moonbit
@testing.assert_enqueued_where(eng, "default", fn(j) {
  j.args == "{\"user_id\":42}"
})
```

## Dashboard

Set `dashboard_port` in your config to enable the built-in web UI:

```moonbit
let config = {
  ..@tide.Config::default(),
  dashboard_port: Some(4567),
}
```

Open **http://localhost:4567**. No separate process, no npm, no build step — the dashboard is an embedded single-page app that starts as a background task inside `Instance::run()`.

Features:
- Live queue stats (available / scheduled / executing)
- Browse jobs by state with full detail view (args, errors, timestamps, attempted-by nodes)
- **Retry** discarded or cancelled jobs
- **Cancel** queued or scheduled jobs
- **Delete** terminal job records
- Auto-refresh every 5 seconds

## Configuration reference

```moonbit
pub struct Config {
  redis_url             : String       // "redis://127.0.0.1:6379"
  queues                : Map[String, Int]  // queue → max concurrency
  pool_size             : Int          // Redis connection pool size (default 5)
  name                  : String       // logical cluster name (default "tide")
  workers               : Map[String, WorkerFn]
  shutdown_grace_ms     : Int          // graceful drain timeout (default 15 000)
  job_timeout_ms        : Int          // per-job execution timeout (default 30 000)
  prune_max_age_ms      : Int64?       // None = disabled; Some(n) = prune after n ms
  lifeline_rescue_after_ms : Int64     // PEL reclaim threshold (default 30 000)
  cron_entries          : Array[CronEntry]
  peer_mode             : PeerMode     // Local (default) or Redis
  notify_cancel         : Bool         // pub/sub cancel signals (default false)
  dashboard_port        : Int?         // None = disabled (default); Some(4567) = enable
}
```

Use `Config::default()` and spread to override only what you need:

```moonbit
let config = {
  ..@tide.Config::default(),
  redis_url: "redis://prod-redis:6379",
  queues: Map([("critical", 20), ("default", 10), ("bulk", 5)]),
  dashboard_port: Some(4567),
}
```

---

## Project structure

```
src/
  redis/       # Pure-MoonBit RESP2 client and connection pool
  job/         # Job struct, state machine, serialisation, priority, tags, unique, meta
  engine/      # Engine open trait + RedisEngine (Lua scripts, XREADGROUP, ZADD)
  queue/       # QueueRunner, executor (timeout + retry logic), ShutdownSignal
  plugin/      # Stager, lifeline, pruner, cron (parser + fire)
  peer/        # Leader election — Local (stub) and Redis (SET NX PX)
  notifier/    # Cancel-signal pub/sub
  outbox/      # Transactional outbox — DbConn trait, Relay, Postgres/SQLite adapters
  migration/   # DDL strings for tide_outbox table (Postgres + SQLite)
  telemetry/   # TelemetryEvent enum + Handler trait
  testing/     # SandboxEngine (in-memory Engine impl) + test helpers
  dashboard/   # Embedded web dashboard — query API, HTTP handler, HTML/JS
  tide/        # Public API surface — Instance, Config, TideError
```

| File pattern | Purpose |
|---|---|
| `*.mbt` | Source |
| `*_wbtest.mbt` | Whitebox tests (access package internals) |
| `*_test.mbt` | Blackbox tests |

## Development

```bash
moon install          # download dependencies
moon build            # compile all packages
moon check            # type-check without building
moon test             # run all unit + integration tests
moon fmt              # format code
moon info             # regenerate .mbti interface files after public API changes
```

Integration tests that hit Redis require a live instance:

```bash
docker compose up -d redis
moon test
```

### Proof toolchain (optional)

`moon prove` verifies `proof_assert` invariants via Why3 + Z3. Required only if you modify proof-carrying code.

**why3 1.7.2** (requires OCaml < 5.4.0):

```bash
opam switch create why3 5.3.0 --yes
opam install why3=1.7.2 --switch=why3 --yes
eval $(opam env --switch=why3)
```

**Z3 4.12.6** (Why3 1.7.2 does not support Z3 4.13+):

```bash
curl -L "https://github.com/Z3Prover/z3/releases/download/z3-4.12.6/z3-4.12.6-arm64-osx-11.0.zip" \
  -o /tmp/z3.zip && unzip /tmp/z3.zip -d /tmp/z3
cp /tmp/z3/z3-4.12.6-arm64-osx-11.0/bin/z3 ~/bin/z3 && chmod +x ~/bin/z3
why3 config detect   # should print: Found prover Z3 version 4.12.6, OK.
moon prove           # all goals pass
```
