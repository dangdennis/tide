// Transactional outbox — caller-facing insert API.
//
// The caller wraps their active DB connection in PgClientConn / PgTxConn /
// SqliteConn and passes it here.  insert() builds the parameterised SQL and
// delegates to DbConn::exec, so the INSERT runs inside whatever transaction
// the caller has open.
//
// Example (Postgres, inside a transaction):
//
//   client.with_transaction(fn(tx) {
//     // … domain writes …
//     @outbox.insert(@outbox.PgTxConn::new(tx), job) catch { ... }
//   })
//
// Column ordering (positions 1–7, one-based for $N placeholders):
//   1. queue
//   2. worker
//   3. args
//   4. tags
//   5. priority
//   6. max_attempts
//   7. scheduled_at  (Integer or Null)

// ── Postgres INSERT SQL ───────────────────────────────────────────────────────

///|
/// Parameterised INSERT for PostgreSQL ($1 … $7 placeholders).
pub const PG_INSERT_SQL : String =
  (
    #|INSERT INTO tide_outbox
    #|  (queue, worker, args, tags, priority, max_attempts, scheduled_at)
    #|VALUES ($1, $2, $3, $4, $5, $6, $7)
  )

// ── SQLite INSERT SQL ─────────────────────────────────────────────────────────

///|
/// Parameterised INSERT for SQLite (? positional placeholders).
pub const SQLITE_INSERT_SQL : String =
  (
    #|INSERT INTO tide_outbox
    #|  (queue, worker, args, tags, priority, max_attempts, scheduled_at)
    #|VALUES (?, ?, ?, ?, ?, ?, ?)
  )

// ── Public API ────────────────────────────────────────────────────────────────

///|
/// Insert `job` into tide_outbox using a Postgres-flavoured parameterised
/// INSERT ($1 … $7).  Call this within the caller's open transaction so that
/// the outbox write is committed atomically with the caller's business logic.
pub async fn[C : DbConn] insert(conn : C, job : @job.Job) -> Unit raise OutboxError {
  insert_with_sql(conn, PG_INSERT_SQL, job)
}

///|
/// Insert `job` into tide_outbox using a SQLite-flavoured parameterised
/// INSERT (? placeholders).  Call this within the caller's open transaction.
pub async fn[C : DbConn] insert_sqlite(
  conn : C,
  job : @job.Job,
) -> Unit raise OutboxError {
  insert_with_sql(conn, SQLITE_INSERT_SQL, job)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

///|
async fn[C : DbConn] insert_with_sql(
  conn : C,
  sql : String,
  job : @job.Job,
) -> Unit raise OutboxError {
  let tags_json = @job.json_string_array(job.tags)
  let sched : DbValue = match job.scheduled_at {
    0L => Null // sentinel: 0 means "not scheduled"
    t => Integer(t)
  }
  let params : Array[DbValue] = [
    Text(job.queue),
    Text(job.worker),
    Text(job.args),
    Text(tags_json),
    Integer(job.priority.to_int64()),
    Integer(job.max_attempts.to_int64()),
    sched,
  ]
  let _ = conn.exec(sql, params)
}
