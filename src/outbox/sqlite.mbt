// SQLite adapter implementing DbConn for the transactional outbox.
//
// Callers wrap an @sqlite3.Connection in SqliteConn to participate in the
// outbox protocol.  col_count controls how many columns query_rows reads
// per result row; default 8 matches the 8-column outbox SELECT used by the
// relay.  For the insert-only path (exec) the field is unused.
//
// Usage (insert inside a business transaction):
//   let db_conn = @outbox.SqliteConn::new(sqlite_conn)
//   @outbox.insert_sqlite(db_conn, job) catch { ... }
//
// Usage (relay):
//   let relay = @outbox.Relay::new(
//     instance=instance,
//     conn=@outbox.SqliteConn::new(relay_conn),
//     claim_sql=@outbox.SQLITE_CLAIM_SQL,
//     delete_sql=@outbox.SQLITE_DELETE_SQL,
//   )

// ── Claim / delete SQL ────────────────────────────────────────────────────────

///|
/// SELECT used by the relay to claim a batch of outbox rows.
/// SQLite has no SKIP LOCKED; a single relay process is assumed.
pub const SQLITE_CLAIM_SQL : String =
  #|SELECT id, queue, worker, args, tags, priority, max_attempts, scheduled_at
  #|FROM tide_outbox
  #|ORDER BY id
  #|LIMIT ?

///|
/// DELETE used by the relay after each row is forwarded to tide.
pub const SQLITE_DELETE_SQL : String = "DELETE FROM tide_outbox WHERE id = ?"

// ── SqliteConn ────────────────────────────────────────────────────────────────

///|
/// Wraps a @sqlite3.Connection so it can be used wherever DbConn is required.
///
/// col_count — number of columns to read per row in query_rows.  Defaults to
/// 8 (the eight columns of the outbox SELECT).  Only matters for query_rows;
/// exec ignores it.
pub struct SqliteConn {
  conn : @sqlite3.Connection
  col_count : Int
}

///|
pub fn SqliteConn::new(conn : @sqlite3.Connection) -> SqliteConn {
  { conn, col_count: 8 }
}

///|
pub fn SqliteConn::with_col_count(
  self : SqliteConn,
  col_count : Int,
) -> SqliteConn {
  { ..self, col_count, }
}

// ── DbConn implementation ─────────────────────────────────────────────────────

///|
pub impl DbConn for SqliteConn with fn exec(self, sql, params) -> Int raise OutboxError {
  let stmt = self.conn.prepare(sql) catch {
    e => raise OutboxError::DbError("sqlite prepare: \{e}")
  }
  sqlite_bind_params(stmt, params) catch {
    e => raise OutboxError::DbError("sqlite bind: \{e}")
  }
  stmt.step_once() catch {
    e => raise OutboxError::DbError("sqlite step: \{e}")
  }
  stmt.finalize() catch {
    e => raise OutboxError::DbError("sqlite finalize: \{e}")
  }
  1
}

///|
/// Read at most self.col_count columns per row, returning each as Text or
/// Null.  Integer columns are coerced to their decimal text representation
/// by SQLite; Null columns produce an empty string which we map to Null.
pub impl DbConn for SqliteConn with fn query_rows(self, sql, params) -> Array[
  Row,
] raise OutboxError {
  let stmt = self.conn.prepare(sql) catch {
    e => raise OutboxError::DbError("sqlite prepare: \{e}")
  }
  sqlite_bind_params(stmt, params) catch {
    e => raise OutboxError::DbError("sqlite bind: \{e}")
  }
  let rows : Array[Row] = []
  let col_count = self.col_count
  let done = Ref(false)
  while !done.val {
    let has_row = stmt.step() catch {
      e => raise OutboxError::DbError("sqlite step: \{e}")
    }
    if !has_row {
      done.val = true
    } else {
      let vals : Array[DbValue] = []
      for i in 0..<col_count {
        let s : String = stmt.column(index=i)
        if s == "" {
          vals.push(Null)
        } else {
          vals.push(Text(s))
        }
      }
      rows.push({ vals, })
    }
  }
  stmt.finalize() catch {
    e => raise OutboxError::DbError("sqlite finalize: \{e}")
  }
  rows
}

// ── Internal helpers ──────────────────────────────────────────────────────────

///|
fn sqlite_bind_params(
  stmt : @sqlite3.Statement,
  params : Array[DbValue],
) -> Unit raise @sqlite3.SqliteError {
  for i in 0..<params.length() {
    let idx = i + 1 // SQLite bind indices are 1-based
    match params[i] {
      Text(s) => stmt.bind(index=idx, s)
      Integer(n) => stmt.bind(index=idx, n)
      Null => () // unbound params default to NULL in SQLite
    }
  }
}
