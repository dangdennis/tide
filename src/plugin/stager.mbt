// Stager: one tick of the scheduled→available promotion loop.
// Call this every ~1 s from a spawn_loop to continuously drain the
// scheduled sorted set into the available sorted set.

///|
pub async fn stager_tick(engine : @engine.RedisEngine, queue : String) -> Unit {
  let _ = engine.stage_scheduled_jobs(queue~) catch { _ => 0 }
  @async.sleep(1000)
}
