// execute runs a single fetched job through its registered worker function.
// On success: completes the job. On error: retries with backoff or discards.
// On snooze: reschedules. On discard: marks discarded.
// Engine errors are propagated to the caller.

///|
pub async fn execute(
  engine : @engine.RedisEngine,
  workers : Map[String, async (@job.Job) -> @worker.PerformResult raise Error],
  fetched : @job.FetchedJob,
  timeout_ms? : Int = 30000,
) -> Unit raise @engine.EngineError {
  let job = fetched.job
  let worker_fn = match workers.get(job.worker) {
    None => {
      engine.discard_job(fetched)
      return
    }
    Some(f) => f
  }
  let result = try? @async.with_timeout_opt(timeout_ms, async fn() {
    worker_fn(job)
  })
  match result {
    Err(e) =>
      if job.attempt + 1 < job.max_attempts {
        let backoff = @job.default_backoff_ms(job.attempt + 1)
        engine.error_job(
          fetched,
          error_msg="\{e}",
          next_scheduled_at=Some(@async.now() + backoff),
        )
      } else {
        engine.error_job(fetched, error_msg="\{e}", next_scheduled_at=None)
      }
    Ok(None) =>
      if job.attempt + 1 < job.max_attempts {
        let backoff = @job.default_backoff_ms(job.attempt + 1)
        engine.error_job(
          fetched,
          error_msg="timeout",
          next_scheduled_at=Some(@async.now() + backoff),
        )
      } else {
        engine.error_job(fetched, error_msg="timeout", next_scheduled_at=None)
      }
    Ok(Some(@worker.PerformResult::Ok)) => engine.complete_job(fetched)
    Ok(Some(@worker.PerformResult::Snooze(secs))) =>
      engine.snooze_job(
        fetched,
        scheduled_at=@async.now() + Int64::from_int(secs) * 1000L,
      )
    Ok(Some(@worker.PerformResult::Discard(_reason))) =>
      engine.discard_job(fetched)
  }
}
