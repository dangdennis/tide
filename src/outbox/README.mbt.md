# outbox

Transactional outbox — insert tide jobs atomically inside an existing database
transaction to solve the dual-write problem.  A background relay polls the
`tide_outbox` table, forwards rows to Redis, and deletes them.

## How it works

1. Your business logic and the job insert share the same DB transaction.
2. If the transaction commits, the relay eventually forwards the job to tide.
3. If the transaction rolls back, the job row disappears with it — no ghost jobs.

## Setup

Apply the schema migration once at startup:

```mbt nocheck
// PostgreSQL
pg_conn.execute(@migration.POSTGRES_DDL)

// SQLite
sqlite_conn.execute(@migration.SQLITE_DDL)
```

## Insert inside a transaction

```mbt nocheck
// PostgreSQL — inside a BEGIN / COMMIT block
let job = @job.Job::new("billing", "ChargeWorker").with_args(args)
@outbox.insert(@outbox.PgTxConn::new(tx), job)
// commit your business logic + the outbox row atomically
```

```mbt nocheck
// SQLite
@outbox.insert_sqlite(@outbox.SqliteConn::new(conn), job)
```

## Start the relay

```mbt nocheck
let relay = @outbox.Relay::new(
  instance=tide_instance,
  conn=@outbox.PgClientConn::new(relay_client),
  claim_sql=@outbox.PG_CLAIM_SQL,
  delete_sql=@outbox.PG_DELETE_SQL,
)

@async.with_task_group(async fn(tg) {
  tg.spawn_bg(allow_failure=true) <| () => {
    tide_instance.run() catch { _ => () }
  }
  tg.spawn_bg(allow_failure=true) <| () => {
    relay.run() catch { _ => () }
  }
})
```

The relay polls `tide_outbox` every 500 ms.  Use unique jobs for
deduplication if at-least-once delivery could cause duplicate processing.

## DbConn trait

Implement `DbConn` to wrap any database connection type:

```mbt nocheck
pub impl @outbox.DbConn for MyConn with
  async fn exec(self, sql, params) -> Int raise @outbox.OutboxError { ... }
  async fn query_rows(self, sql, params) -> Array[@outbox.Row] raise @outbox.OutboxError { ... }
```
