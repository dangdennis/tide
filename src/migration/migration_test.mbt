// Unit tests for migration DDL constants: verify key structural substrings so
// accidental truncation or rename is caught at test time.

///|
test "SQLITE_DDL contains CREATE TABLE IF NOT EXISTS tide_outbox" {
  assert_true(SQLITE_DDL.contains("CREATE TABLE IF NOT EXISTS tide_outbox"))
}

///|
test "SQLITE_DDL defines expected columns" {
  assert_true(SQLITE_DDL.contains("queue"))
  assert_true(SQLITE_DDL.contains("worker"))
  assert_true(SQLITE_DDL.contains("args"))
  assert_true(SQLITE_DDL.contains("priority"))
  assert_true(SQLITE_DDL.contains("max_attempts"))
  assert_true(SQLITE_DDL.contains("scheduled_at"))
}

///|
test "SQLITE_DDL creates index on id" {
  assert_true(
    SQLITE_DDL.contains("CREATE INDEX IF NOT EXISTS tide_outbox_id_idx"),
  )
}

///|
test "POSTGRES_DDL contains CREATE TABLE IF NOT EXISTS tide_outbox" {
  assert_true(POSTGRES_DDL.contains("CREATE TABLE IF NOT EXISTS tide_outbox"))
}

///|
test "POSTGRES_DDL defines expected columns" {
  assert_true(POSTGRES_DDL.contains("queue"))
  assert_true(POSTGRES_DDL.contains("worker"))
  assert_true(POSTGRES_DDL.contains("args"))
  assert_true(POSTGRES_DDL.contains("priority"))
  assert_true(POSTGRES_DDL.contains("max_attempts"))
  assert_true(POSTGRES_DDL.contains("scheduled_at"))
}

///|
test "POSTGRES_DDL creates index on id" {
  assert_true(
    POSTGRES_DDL.contains("CREATE INDEX IF NOT EXISTS tide_outbox_id_idx"),
  )
}

///|
test "SQLITE_DDL uses AUTOINCREMENT for primary key" {
  assert_true(SQLITE_DDL.contains("AUTOINCREMENT"))
}

///|
test "POSTGRES_DDL uses BIGSERIAL for primary key" {
  assert_true(POSTGRES_DDL.contains("BIGSERIAL"))
}
