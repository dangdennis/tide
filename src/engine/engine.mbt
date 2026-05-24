// Engine trait: storage abstraction for tide.
// Only implementation is RedisEngine; the trait isolates queue/executor logic
// from Redis-specific details and enables test doubles.

pub(open) trait Engine {
  // Write a new job hash + add to the available sorted set.
  // Populates id, inserted_at, and scheduled_at before persisting.
  async fn insert_job(Self, @job.Job) -> @job.Job raise EngineError

  // Insert multiple jobs. Not transactional across jobs.
  async fn insert_all(Self, Array[@job.Job]) -> Array[@job.Job] raise EngineError

  // Move up to `limit` jobs from the available sorted set into the delivery
  // stream (move_to_stream.lua), then claim them via XREADGROUP.
  async fn fetch_jobs(
    Self,
    queue~ : String,
    limit~ : Int,
  ) -> Array[@job.FetchedJob] raise EngineError

  // XACK + mark hash state "completed".
  async fn complete_job(Self, @job.FetchedJob) -> Unit raise EngineError

  // XACK + record error in hash.
  // Some(ts) → increment attempt + ZADD available for retry.
  // None     → mark discarded.
  async fn error_job(
    Self,
    @job.FetchedJob,
    error_msg~ : String,
    next_scheduled_at~ : Option[Int64],
  ) -> Unit raise EngineError

  // XACK + ZADD to scheduled sorted set. Attempt count unchanged.
  async fn snooze_job(
    Self,
    @job.FetchedJob,
    scheduled_at~ : Int64,
  ) -> Unit raise EngineError

  // XACK + mark hash state "discarded" immediately (executor chose Discard).
  async fn discard_job(Self, @job.FetchedJob) -> Unit raise EngineError

  // User-facing cancel: XACK (if in PEL) or ZREM, mark hash cancelled.
  async fn cancel_job(Self, id~ : String) -> Unit raise EngineError

  // User-facing retry: move a discarded/cancelled job back to available.
  async fn retry_job(Self, id~ : String) -> Unit raise EngineError

  // Delete job hashes + sorted-set entries older than `older_than_ms`.
  // Returns count of pruned jobs.
  async fn prune_jobs(
    Self,
    queue~ : String,
    older_than_ms~ : Int64,
  ) -> Int raise EngineError

  // XGROUP CREATE MKSTREAM — idempotent; call once per queue at startup.
  async fn ensure_stream_group(Self, queue~ : String) -> Unit raise EngineError

  // SCRIPT LOAD the two Lua scripts. Call before insert_job / fetch_jobs.
  async fn ensure_scripts(Self) -> Unit raise EngineError
}

pub suberror EngineError {
  RedisFailure(String)
  JobNotFound(String)
  InvalidJobData(String)
}

pub impl Show for EngineError with output(self, logger) {
  match self {
    RedisFailure(msg) => logger.write_string("RedisFailure(\{msg})")
    JobNotFound(id) => logger.write_string("JobNotFound(\{id})")
    InvalidJobData(msg) => logger.write_string("InvalidJobData(\{msg})")
  }
}
