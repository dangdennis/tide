// Unit tests for the transactional outbox.
//
// These tests use an in-memory SQLite database — no external service required.
// They verify:
//   1. insert_sqlite() inserts a job row with the correct field values.
//   2. query_rows reads the row back as the expected DbValues.
//   3. parse_outbox_row reconstructs an OutboxRow accurately.
//   4. outbox_row_to_job produces a Job with the expected fields.

// DDL for the test database (minimal, no indexes needed for tests).
let test_ddl : String =
  (
    #|CREATE TABLE IF NOT EXISTS tide_outbox (
    #|  id           INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    #|  queue        TEXT    NOT NULL,
    #|  worker       TEXT    NOT NULL,
    #|  args         TEXT    NOT NULL DEFAULT '{}',
    #|  tags         TEXT    NOT NULL DEFAULT '[]',
    #|  priority     INTEGER NOT NULL DEFAULT 5,
    #|  max_attempts INTEGER NOT NULL DEFAULT 3,
    #|  scheduled_at INTEGER,
    #|  inserted_at  INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000)
    #|)
  )

// ── Helper ────────────────────────────────────────────────────────────────────

fn open_test_db() -> (@sqlite3.Connection, SqliteConn) raise @sqlite3.SqliteError {
  let conn = @sqlite3.Connection::open(":memory:")
  let stmt = conn.prepare(test_ddl)
  stmt.step_once()
  stmt.finalize()
  (conn, SqliteConn::new(conn))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

///|
async test "insert_sqlite: basic job is stored" {
  let (conn, db) = open_test_db() catch { e => raise e }
  let job = @job.Job::new("default", "MyWorker")
    .with_args("{\"x\":1}")
    .with_tags(["billing"])
    .with_priority(3)
    .with_max_attempts(5)
  insert_sqlite(db, job) catch { e => { conn.close(); raise e } }
  let rows = db.query_rows(
    "SELECT id, queue, worker, args, tags, priority, max_attempts, scheduled_at FROM tide_outbox",
    [],
  ) catch { e => { conn.close(); raise e } }
  assert_eq(rows.length(), 1)
  let v = rows[0].vals
  // index 0: id = "1" (autoincrement)
  assert_eq(v[1], Text("default"))
  assert_eq(v[2], Text("MyWorker"))
  assert_eq(v[3], Text("{\"x\":1}"))
  assert_eq(v[4], Text("[\"billing\"]"))
  assert_eq(v[5], Text("3"))
  assert_eq(v[6], Text("5"))
  assert_eq(v[7], Null) // no scheduled_at
  conn.close()
}

///|
async test "insert_sqlite: scheduled_at is stored as epoch ms" {
  let (conn, db) = open_test_db() catch { e => raise e }
  let job = @job.Job::new("q", "W").with_scheduled_at(1700000000000L)
  insert_sqlite(db, job) catch { e => { conn.close(); raise e } }
  let rows = db.query_rows(
    "SELECT scheduled_at FROM tide_outbox",
    [],
  ) catch { e => { conn.close(); raise e } }
  assert_eq(rows.length(), 1)
  assert_eq(rows[0].vals[0], Text("1700000000000"))
  conn.close()
}

///|
async test "parse_outbox_row: round-trips all fields" {
  let (conn, db) = open_test_db() catch { e => raise e }
  let job = @job.Job::new("emails", "SendEmailWorker")
    .with_args("{\"to\":\"a@b.com\"}")
    .with_tags(["notify", "transactional"])
    .with_priority(2)
    .with_max_attempts(4)
    .with_scheduled_at(1700000000000L)
  insert_sqlite(db, job) catch { e => { conn.close(); raise e } }
  let rows = db.query_rows(
    "SELECT id, queue, worker, args, tags, priority, max_attempts, scheduled_at FROM tide_outbox",
    [],
  ) catch { e => { conn.close(); raise e } }
  assert_eq(rows.length(), 1)
  let orow = parse_outbox_row(rows[0])
  assert_eq(orow.queue,        "emails")
  assert_eq(orow.worker,       "SendEmailWorker")
  assert_eq(orow.args,         "{\"to\":\"a@b.com\"}")
  assert_eq(orow.tags,         "[\"notify\",\"transactional\"]")
  assert_eq(orow.priority,     2)
  assert_eq(orow.max_attempts, 4)
  debug_inspect(orow.scheduled_at, content="Some(1700000000000)")
  conn.close()
}

///|
async test "outbox_row_to_job: builds a correct Job" {
  let (conn, db) = open_test_db() catch { e => raise e }
  let job = @job.Job::new("billing", "ChargeWorker")
    .with_args("{\"amount\":100}")
    .with_tags(["charge"])
    .with_priority(1)
    .with_max_attempts(3)
  insert_sqlite(db, job) catch { e => { conn.close(); raise e } }
  let rows = db.query_rows(
    "SELECT id, queue, worker, args, tags, priority, max_attempts, scheduled_at FROM tide_outbox",
    [],
  ) catch { e => { conn.close(); raise e } }
  let orow = parse_outbox_row(rows[0])
  let rebuilt = outbox_row_to_job(orow)
  assert_eq(rebuilt.queue,        "billing")
  assert_eq(rebuilt.worker,       "ChargeWorker")
  assert_eq(rebuilt.args,         "{\"amount\":100}")
  debug_inspect(rebuilt.tags, content="[\"charge\"]")
  assert_eq(rebuilt.priority,     1)
  assert_eq(rebuilt.max_attempts, 3)
  assert_eq(rebuilt.scheduled_at, 0L) // no scheduled_at → stays 0
  conn.close()
}

///|
async test "exec: DELETE removes the row" {
  let (conn, db) = open_test_db() catch { e => raise e }
  let job = @job.Job::new("q", "W")
  insert_sqlite(db, job) catch { e => { conn.close(); raise e } }
  // Read back to get the id
  let rows = db.query_rows(
    "SELECT id FROM tide_outbox",
    [],
  ) catch { e => { conn.close(); raise e } }
  assert_eq(rows.length(), 1)
  let id_text = match rows[0].vals[0] {
    Text(s) => s
    _ => "0"
  }
  let id = @string.parse_int64(id_text, base=10) catch { _ => 0L }
  // Delete via exec
  let _ = db.exec(SQLITE_DELETE_SQL, [Integer(id)]) catch {
    e => { conn.close(); raise e }
  }
  let after = db.query_rows("SELECT id FROM tide_outbox", []) catch {
    e => { conn.close(); raise e }
  }
  assert_eq(after.length(), 0)
  conn.close()
}
