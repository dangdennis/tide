// PostgreSQL DDL for the tide_outbox table.
//
// Run this once at application startup or via your migration tool before
// starting the relay. The statements are idempotent (CREATE IF NOT EXISTS).
//
// tide_outbox columns
// ─────────────────────────────────────────────────────────────────────
//   id           — auto-incrementing primary key; used for FIFO ordering
//   queue        — target tide queue name
//   worker       — worker function name registered on the tide Instance
//   args         — JSON payload forwarded verbatim to the job
//   tags         — JSON array string e.g. '["billing","notify"]'
//   priority     — 0–9 (lower = higher priority); default 5
//   max_attempts — maximum retry count; default 3
//   scheduled_at — epoch milliseconds; NULL means "run immediately"
//   inserted_at  — wall-clock timestamp; used only for observability

///|
pub const POSTGRES_DDL : String =
  (
    #|CREATE TABLE IF NOT EXISTS tide_outbox (
    #|  id           BIGSERIAL   NOT NULL PRIMARY KEY,
    #|  queue        TEXT        NOT NULL,
    #|  worker       TEXT        NOT NULL,
    #|  args         TEXT        NOT NULL DEFAULT '{}',
    #|  tags         TEXT        NOT NULL DEFAULT '[]',
    #|  priority     INT         NOT NULL DEFAULT 5,
    #|  max_attempts INT         NOT NULL DEFAULT 3,
    #|  scheduled_at BIGINT,
    #|  inserted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    #|);
    #|CREATE INDEX IF NOT EXISTS tide_outbox_id_idx
    #|  ON tide_outbox (id);
  )
