// Outbox relay — polls tide_outbox and forwards rows to a tide Instance.
//
// The relay is the second half of the transactional outbox pattern.  It
// runs as a long-lived loop alongside the tide Instance, claims batches of
// rows from tide_outbox, forwards each one to tide (Redis) via
// Instance::insert, and deletes the row once the insert succeeds.
//
// Usage:
//   let relay = @outbox.Relay::new(
//     instance=tide_instance,
//     conn=@outbox.PgClientConn::new(relay_client),
//     claim_sql=@outbox.PG_CLAIM_SQL,
//     delete_sql=@outbox.PG_DELETE_SQL,
//   )
//   @async.with_task_group(async fn(tg) {
//     tg.spawn_bg(allow_failure=true) <| () => { tide_instance.run() catch { _ => () } }
//     tg.spawn_bg(allow_failure=true) <| () => { relay.run()          catch { _ => () } }
//   })

// ── Relay ─────────────────────────────────────────────────────────────────────

///|
/// Polls tide_outbox and forwards each row to a tide Instance.
///
/// C — any DbConn implementation (SqliteConn, PgClientConn, …).
/// claim_sql  — DB-specific SELECT statement (SQLITE_CLAIM_SQL or PG_CLAIM_SQL).
/// delete_sql — DB-specific DELETE statement (SQLITE_DELETE_SQL or PG_DELETE_SQL).
pub struct Relay[C] {
  /// The tide instance that will receive the forwarded jobs.
  instance : @tide.Instance
  /// Long-lived database connection used for all poll-cycle queries.
  conn : C
  /// How long to sleep (ms) when the outbox is empty.
  poll_ms : Int
  /// Maximum rows to claim per poll cycle.
  batch_size : Int
  /// DB-specific SELECT … LIMIT ? statement.
  claim_sql : String
  /// DB-specific DELETE … WHERE id = ? statement.
  delete_sql : String
}

///|
pub fn[C] Relay::new(
  instance : @tide.Instance,
  conn : C,
  claim_sql : String,
  delete_sql : String,
  poll_ms? : Int = 500,
  batch_size? : Int = 100,
) -> Relay[C] {
  { instance, conn, poll_ms, batch_size, claim_sql, delete_sql }
}

// ── Main loop ─────────────────────────────────────────────────────────────────

///|
/// Run the relay loop indefinitely.
///
/// On each iteration:
///   1. Claim up to batch_size rows.
///   2. For each row: parse → build Job → Instance::insert → delete.
///   3. If the batch was empty, sleep for poll_ms before the next cycle.
///   4. If the batch was full, skip the sleep and drain as fast as possible.
pub async fn[C : DbConn] Relay::run(self : Relay[C]) -> Unit raise OutboxError {
  while true {
    let rows = self.conn.query_rows(self.claim_sql, [
      Integer(self.batch_size.to_int64()),
    ])
    let row_count = rows.length()
    for row in rows {
      let outbox_row = parse_outbox_row(row)
      let job = outbox_row_to_job(outbox_row)
      let _ = self.instance.insert(job) catch {
        e => raise OutboxError::ForwardError("tide insert: \{e}")
      }
      let _ = self.conn.exec(self.delete_sql, [Integer(outbox_row.id)])
    }
    // Sleep only when the outbox was empty; drain continuously otherwise.
    if row_count == 0 {
      try @async.sleep(self.poll_ms) catch {
        _ => ()
      } noraise {
        _ => ()
      }
    }
  }
}

// ── Row parsing ───────────────────────────────────────────────────────────────

///|
/// Map one `Row` from the claim SELECT into a typed `OutboxRow`.
///
/// Column order matches SQLITE_CLAIM_SQL / PG_CLAIM_SQL:
///   0  id            TEXT  → Int64
///   1  queue         TEXT
///   2  worker        TEXT
///   3  args          TEXT
///   4  tags          TEXT  (JSON array string)
///   5  priority      TEXT  → Int
///   6  max_attempts  TEXT  → Int
///   7  scheduled_at  TEXT | Null → Int64?
fn parse_outbox_row(row : Row) -> OutboxRow {
  let vals = row.vals
  let id = col_int64(vals, 0)
  let queue = col_text(vals, 1)
  let worker = col_text(vals, 2)
  let args = col_text(vals, 3)
  let tags = col_text(vals, 4)
  let priority = col_int64(vals, 5).to_int()
  let max_attempts = col_int64(vals, 6).to_int()
  let scheduled_at : Int64? = match vals.get(7) {
    Some(Null) => None
    Some(Text("")) => None
    Some(Text(s)) => Some(parse_int64_or_zero(s))
    Some(Integer(0L)) => None
    Some(Integer(t)) => Some(t)
    None => None
  }
  { id, queue, worker, args, tags, priority, max_attempts, scheduled_at }
}

///|
/// Build a @job.Job from an OutboxRow, ready for Instance::insert.
fn outbox_row_to_job(r : OutboxRow) -> @job.Job {
  let tags = @job.parse_json_string_array(r.tags)
  let job = @job.Job::new(r.queue, r.worker)
    .with_args(r.args)
    .with_tags(tags)
    .with_priority(r.priority)
    .with_max_attempts(r.max_attempts)
  match r.scheduled_at {
    Some(t) => job.with_scheduled_at(t)
    None => job
  }
}

// ── Column helpers ────────────────────────────────────────────────────────────

///|
fn col_text(vals : Array[DbValue], i : Int) -> String {
  match vals.get(i) {
    Some(Text(s)) => s
    _ => ""
  }
}

///|
fn col_int64(vals : Array[DbValue], i : Int) -> Int64 {
  match vals.get(i) {
    Some(Integer(n)) => n
    Some(Text(s)) => parse_int64_or_zero(s)
    _ => 0L
  }
}

///|
fn parse_int64_or_zero(s : String) -> Int64 {
  @string.parse_int64(s, base=10) catch {
    _ => 0L
  }
}
