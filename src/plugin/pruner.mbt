// Pruner: one tick of the terminal-job cleanup loop.
// Call this every `interval_ms` to delete completed/discarded/cancelled jobs
// older than `max_age_ms`.

///|
pub async fn pruner_tick(
  engine : @engine.RedisEngine,
  queue : String,
  max_age_ms : Int64,
  interval_ms : Int,
) -> Unit {
  let _ = engine.prune_jobs(queue~, older_than_ms=max_age_ms) catch { _ => 0 }
  @async.sleep(interval_ms)
}
