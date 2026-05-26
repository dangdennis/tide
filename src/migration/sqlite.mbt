// SQLite DDL for the tide_outbox table.
//
// Run this once when opening the database (e.g. in your application startup).
// The statements are idempotent (CREATE IF NOT EXISTS).
//
// SQLite-specific notes
// ─────────────────────────────────────────────────────────────────────
//   • tags is stored as a TEXT JSON array (e.g. '["billing","notify"]')
//     rather than a native array type, which SQLite does not support.
//   • scheduled_at is epoch milliseconds stored as INTEGER.
//   • inserted_at is epoch milliseconds via unixepoch('now') * 1000.

///|
/// Idempotent DDL to create the `tide_outbox` table and its index in SQLite.
/// Execute this once when opening the database before starting the relay.
pub const SQLITE_DDL : String =
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
  #|);
  #|CREATE INDEX IF NOT EXISTS tide_outbox_id_idx
  #|  ON tide_outbox (id);
