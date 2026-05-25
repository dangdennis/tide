// Lifeline: one tick of the PEL-reclaim loop.
// Call this every `rescue_after_ms` from a spawn_loop to recover jobs
// that were in-flight when a worker node crashed.

///|
pub async fn lifeline_tick(
  engine : @engine.RedisEngine,
  queue : String,
  rescue_after_ms : Int64,
) -> Unit {
  let _ = engine.reclaim_jobs(queue~, rescue_after_ms~) catch { _ => 0 }
  @async.sleep(rescue_after_ms.clamp(min=0L, max=2147483647L).to_int())
}
