// QueueRunner holds the per-queue configuration used by the fetch loop
// inside Instance::run. The actual spawn_loop / spawn_bg calls live in
// tide.mbt because TaskGroup is internal to moonbitlang/async.

///|
pub(all) struct QueueRunner {
  engine : @engine.RedisEngine
  workers : Map[String, async (@job.Job) -> @worker.PerformResult raise Error]
  queue : String
  concurrency : Int
  shutdown : ShutdownSignal
}

///|
pub fn QueueRunner::new(
  engine : @engine.RedisEngine,
  workers : Map[String, async (@job.Job) -> @worker.PerformResult raise Error],
  queue : String,
  concurrency : Int,
  shutdown : ShutdownSignal,
) -> QueueRunner {
  { engine, workers, queue, concurrency, shutdown }
}

// fetch_batch runs one fetch iteration: moves available jobs into the Redis
// Stream and claims them via XREADGROUP. Returns an empty array if stopped.

///|
pub async fn QueueRunner::fetch_batch(
  self : QueueRunner,
) -> Array[@job.FetchedJob] raise @engine.EngineError {
  if self.shutdown.is_stopped() {
    return []
  }
  self.engine.fetch_jobs(queue=self.queue, limit=self.concurrency)
}
