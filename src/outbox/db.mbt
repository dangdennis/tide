// DbConn trait and supporting types for the transactional outbox.
//
// The DbConn trait is the bridge between tide's outbox API and a caller-owned
// database connection.  The caller wraps their existing connection in a concrete
// type (PgClientConn, PgTxConn, or SqliteConn) so that tide can INSERT into
// tide_outbox within the same transaction as the caller's business logic.

///|
/// A SQL parameter value.
pub(all) enum DbValue {
  /// UTF-8 text / varchar.
  Text(String)
  /// 64-bit integer (maps to BIGINT / BIGSERIAL / INTEGER).
  Integer(Int64)
  /// SQL NULL.
  Null
} derive(Debug, Eq)

///|
pub impl Show for DbValue with fn output(self, logger) {
  match self {
    Text(s) => logger.write_string("Text(\{s})")
    Integer(n) => logger.write_string("Integer(\{n})")
    Null => logger.write_string("Null")
  }
}

///|
/// One row returned by DbConn::query_rows.
pub struct Row {
  /// Column values in SELECT order.
  vals : Array[DbValue]
} derive(Debug)

///|
/// One row deserialized from tide_outbox.
pub struct OutboxRow {
  id : Int64
  queue : String
  worker : String
  args : String
  tags : String // raw JSON array string e.g. '["a","b"]'
  priority : Int
  max_attempts : Int
  scheduled_at : Int64? // None = run immediately
} derive(Debug)

///|
pub suberror OutboxError {
  /// A database operation failed.
  DbError(String)
  /// Forwarding a row to the tide engine failed.
  ForwardError(String)
}

///|
pub impl Show for OutboxError with fn output(self, logger) {
  match self {
    DbError(msg) => logger.write_string("OutboxError::DbError(\{msg})")
    ForwardError(msg) =>
      logger.write_string("OutboxError::ForwardError(\{msg})")
  }
}

///|
/// A minimal database connection interface for outbox operations.
///
/// Callers wrap their existing connection (e.g. @pgpool.Client or
/// @sqlite3.Connection) in a concrete adapter struct that implements this
/// trait.  tide's outbox functions accept any DbConn, so the job INSERT runs
/// in the same transaction as the caller's business logic without tide needing
/// to know about the underlying DB client.
pub(open) trait DbConn {
  /// Execute a parameterised DML statement (INSERT / UPDATE / DELETE).
  /// Returns the number of rows affected.
  async fn exec(Self, sql : String, params : Array[DbValue]) -> Int raise OutboxError

  /// Execute a parameterised SELECT.
  /// Returns all result rows.
  async fn query_rows(Self, sql : String, params : Array[DbValue]) -> Array[Row] raise OutboxError
}
