// PostgreSQL adapter implementing DbConn for the transactional outbox.
//
// Callers wrap a @pgpool.Client in PgClientConn to participate in the outbox
// protocol.  A @pgpool.Transaction wraps a live DB transaction, which is the
// common case: the outbox INSERT must run inside the caller's business
// transaction so that both the domain change and the job record are committed
// or rolled back together.
//
// Usage (insert inside a business transaction):
//   pool.with_client(fn(client) {
//     client.with_transaction(fn(tx) {
//       // … domain writes …
//       @outbox.insert(@outbox.PgTxConn::new(tx), job) catch { ... }
//     })
//   })
//
// Usage (relay, using a non-transactional client):
//   let relay = @outbox.Relay::new(
//     instance=instance,
//     conn=@outbox.PgClientConn::new(relay_client),
//     claim_sql=@outbox.PG_CLAIM_SQL,
//     delete_sql=@outbox.PG_DELETE_SQL,
//   )

// ── Claim / delete SQL ────────────────────────────────────────────────────────

///|
/// SELECT … FOR UPDATE SKIP LOCKED allows multiple concurrent relay processes.
pub const PG_CLAIM_SQL : String =
  #|SELECT id, queue, worker, args, tags, priority, max_attempts, scheduled_at
  #|FROM tide_outbox
  #|ORDER BY id
  #|LIMIT $1
  #|FOR UPDATE SKIP LOCKED

///|
/// DELETE used by the relay after each row is forwarded to tide.
pub const PG_DELETE_SQL : String = "DELETE FROM tide_outbox WHERE id = $1"

// ── PgClientConn ─────────────────────────────────────────────────────────────

///|
/// Wraps a @pgpool.Client for use as a DbConn (non-transactional path).
pub struct PgClientConn {
  client : @pgpool.Client
}

///|
pub fn PgClientConn::new(client : @pgpool.Client) -> PgClientConn {
  { client, }
}

///|
pub impl DbConn for PgClientConn with fn exec(self, sql, params) -> Int raise OutboxError {
  let pg_params = db_values_to_pg(params)
  self.client.execute(sql, params=pg_params) catch {
    e => raise OutboxError::DbError("pg execute: \{e}")
  }
}

///|
pub impl DbConn for PgClientConn with fn query_rows(self, sql, params) -> Array[
  Row,
] raise OutboxError {
  let pg_params = db_values_to_pg(params)
  let pg_rows = self.client.query_all(sql, params=pg_params) catch {
    e => raise OutboxError::DbError("pg query: \{e}")
  }
  pg_rows.map(fn(r) { pg_row_to_row(r) })
}

// ── PgTxConn ─────────────────────────────────────────────────────────────────

///|
/// Wraps a @pgpool.Transaction for use as a DbConn.
/// Use this to INSERT into tide_outbox inside the caller's open transaction
/// so that the outbox write is atomically committed with the business logic.
pub struct PgTxConn {
  tx : @pgpool.Transaction
}

///|
pub fn PgTxConn::new(tx : @pgpool.Transaction) -> PgTxConn {
  { tx, }
}

///|
pub impl DbConn for PgTxConn with fn exec(self, sql, params) -> Int raise OutboxError {
  let pg_params = db_values_to_pg(params)
  self.tx.execute(sql, params=pg_params) catch {
    e => raise OutboxError::DbError("pg tx execute: \{e}")
  }
}

///|
pub impl DbConn for PgTxConn with fn query_rows(self, sql, params) -> Array[Row] raise OutboxError {
  let pg_params = db_values_to_pg(params)
  let pg_rows = self.tx.query_all(sql, params=pg_params) catch {
    e => raise OutboxError::DbError("pg tx query: \{e}")
  }
  pg_rows.map(fn(r) { pg_row_to_row(r) })
}

// ── Internal helpers ──────────────────────────────────────────────────────────

///|
/// Convert Array[DbValue] → Array[&@client.ToSql] for pgpool calls.
fn db_values_to_pg(params : Array[DbValue]) -> Array[&@client.ToSql] {
  let out : Array[&@client.ToSql] = []
  for p in params {
    match p {
      Text(s) => out.push(s as &@client.ToSql)
      Integer(n) => out.push(n as &@client.ToSql)
      Null => out.push((None : String?) as &@client.ToSql)
    }
  }
  out
}

///|
/// Convert one @client.Row → Row, reading every column as Text (or Null).
fn pg_row_to_row(row : @client.Row) -> Row {
  let n = row.len()
  let vals : Array[DbValue] = []
  for i in 0..<n {
    let cell = row.get_text(i) catch { _ => None }
    match cell {
      Some(s) => vals.push(Text(s))
      None => vals.push(Null)
    }
  }
  { vals, }
}
