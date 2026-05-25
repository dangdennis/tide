// Cron plugin: one tick of the per-minute cron scheduler.
//
// `cron_fire` checks whether `entry` matches the minute aligned to
// `tick_ms` (a Unix-ms timestamp rounded down to the nearest minute)
// and, if so, inserts the job via the idempotent cron_insert engine call.
//
// `CronEntry` is the user-facing configuration struct.

///|
pub(all) struct CronEntry {
  schedule : String // cron expression, e.g. "0 9 * * 1-5" or "@daily"
  queue : String
  worker : String
  args : String // JSON-encoded job args payload (default "{}")
}

///|
pub fn CronEntry::new(
  schedule : String,
  queue : String,
  worker : String,
) -> CronEntry {
  { schedule, queue, worker, args: "{}" }
}

///|
pub fn CronEntry::with_args(self : CronEntry, args : String) -> CronEntry {
  { ..self, args, }
}

///|
/// Fire a single cron entry if its schedule matches `tick_ms`.
/// Returns true if a job was inserted (or skipped due to an existing
/// unique key from another node).  Silently skips invalid cron expressions.
pub async fn cron_fire(
  engine : @engine.RedisEngine,
  entry : CronEntry,
  tick_ms : Int64,
) -> Bool raise @engine.EngineError {
  let expr = @cron.CronExpr::parse(entry.schedule) catch { _ => return false }
  if !expr.matches_ms(tick_ms) {
    return false
  }
  engine.cron_insert(entry.queue, entry.worker, entry.args, tick_ms)
}
