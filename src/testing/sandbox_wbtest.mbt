// Unit tests for SandboxEngine and testing helpers.
// No external services required.

// ── SandboxEngine ─────────────────────────────────────────────────────────────

///|
async test "insert_job: assigns id and adds to available queue" {
  let eng = SandboxEngine::new()
  let job = @job.Job::new("default", "MyWorker")
  let inserted = eng.insert_job(job)
  assert_eq(inserted.id, "sandbox_1")
  assert_eq(inserted.queue, "default")
  assert_eq(inserted.worker, "MyWorker")
  assert_eq(eng.job_count(), 1)
  // Should appear in available queue.
  assert_eq(assert_enqueued(eng, "default", "MyWorker"), true)
}

///|
async test "fetch_jobs: marks jobs as Executing and removes from available" {
  let eng = SandboxEngine::new()
  let _ = eng.insert_job(@job.Job::new("q", "W"))
  let fetched = eng.fetch_jobs(queue="q", limit=10)
  assert_eq(fetched.length(), 1)
  assert_eq(fetched[0].job.state, @job.Executing)
  // No longer in available queue.
  assert_eq(assert_enqueued(eng, "q", "W"), false)
}

///|
async test "complete_job: marks job as Completed" {
  let eng = SandboxEngine::new()
  let _ = eng.insert_job(@job.Job::new("q", "W"))
  let fetched = eng.fetch_jobs(queue="q", limit=1)
  assert_eq(fetched.length(), 1)
  eng.complete_job(fetched[0])
  let completed = eng.jobs_in_state(@job.Completed)
  assert_eq(completed.length(), 1)
}

///|
async test "error_job with retry: job goes back to available" {
  let eng = SandboxEngine::new()
  let _ = eng.insert_job(@job.Job::new("q", "W"))
  let fetched = eng.fetch_jobs(queue="q", limit=1)
  eng.error_job(fetched[0], error_msg="boom", next_scheduled_at=Some(0L))
  let retryable = eng.jobs_in_state(@job.Retryable)
  assert_eq(retryable.length(), 1)
  assert_eq(assert_enqueued(eng, "q", "W"), true)
}

///|
async test "error_job without retry: job is Discarded" {
  let eng = SandboxEngine::new()
  let _ = eng.insert_job(@job.Job::new("q", "W"))
  let fetched = eng.fetch_jobs(queue="q", limit=1)
  eng.error_job(fetched[0], error_msg="fatal", next_scheduled_at=None)
  let discarded = eng.jobs_in_state(@job.Discarded)
  assert_eq(discarded.length(), 1)
}

///|
async test "discard_job: marks job as Discarded" {
  let eng = SandboxEngine::new()
  let _ = eng.insert_job(@job.Job::new("q", "W"))
  let fetched = eng.fetch_jobs(queue="q", limit=1)
  eng.discard_job(fetched[0])
  let discarded = eng.jobs_in_state(@job.Discarded)
  assert_eq(discarded.length(), 1)
}

///|
async test "cancel_job: marks job as Cancelled and removes from queue" {
  let eng = SandboxEngine::new()
  let inserted = eng.insert_job(@job.Job::new("q", "W"))
  eng.cancel_job(id=inserted.id)
  let cancelled = eng.jobs_in_state(@job.Cancelled)
  assert_eq(cancelled.length(), 1)
  assert_eq(assert_enqueued(eng, "q", "W"), false)
}

///|
async test "retry_job: moves Discarded job back to available" {
  let eng = SandboxEngine::new()
  let _ = eng.insert_job(@job.Job::new("q", "W"))
  let fetched = eng.fetch_jobs(queue="q", limit=1)
  eng.discard_job(fetched[0])
  eng.retry_job(id=fetched[0].job.id)
  assert_eq(assert_enqueued(eng, "q", "W"), true)
}

///|
async test "insert_unique_job: second insert with same key returns AlreadyExists" {
  let eng = SandboxEngine::new()
  let job = @job.Job::new("q", "W")
  let r1 = eng.insert_unique_job(job, unique_key="my-key", period_ms=60000L)
  let r2 = eng.insert_unique_job(job, unique_key="my-key", period_ms=60000L)
  match r1 {
    @engine.UniqueInsertResult::Inserted(_) => ()
    @engine.UniqueInsertResult::AlreadyExists => abort("expected Inserted")
  }
  match r2 {
    @engine.UniqueInsertResult::AlreadyExists => ()
    @engine.UniqueInsertResult::Inserted(_) => abort("expected AlreadyExists")
  }
}

///|
async test "cancel_jobs_by_tag: cancels matching jobs" {
  let eng = SandboxEngine::new()
  let _ = eng.insert_job(@job.Job::new("q", "W").with_tags(["billing"]))
  let _ = eng.insert_job(@job.Job::new("q", "W2").with_tags(["other"]))
  let ids = eng.cancel_jobs_by_tag(queue="q", tag="billing")
  assert_eq(ids.length(), 1)
  let cancelled = eng.jobs_in_state(@job.Cancelled)
  assert_eq(cancelled.length(), 1)
  assert_eq(cancelled[0].worker, "W")
}

///|
async test "retry_jobs_by_tag: re-enqueues matching discarded jobs" {
  let eng = SandboxEngine::new()
  let _ = eng.insert_job(@job.Job::new("q", "W").with_tags(["retry-me"]))
  let fetched = eng.fetch_jobs(queue="q", limit=1)
  eng.discard_job(fetched[0])
  let count = eng.retry_jobs_by_tag(queue="q", tag="retry-me")
  assert_eq(count, 1)
  assert_eq(assert_enqueued(eng, "q", "W"), true)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

///|
async test "perform_inline: calls worker and returns result" {
  let workers : Map[
    String,
    async (@job.Job) -> @worker.PerformResult raise Error,
  ] = Map([("EchoWorker", fn(_job) { @worker.PerformResult::Ok })])
  let job = @job.Job::new("q", "EchoWorker")
  let result = perform_inline(workers, job)
  match result {
    @worker.PerformResult::Ok => ()
    _ => abort("expected Ok")
  }
}

///|
async test "perform_inline: returns Discard for missing worker" {
  let workers : Map[
    String,
    async (@job.Job) -> @worker.PerformResult raise Error,
  ] = Map([])
  let job = @job.Job::new("q", "Missing")
  let result = perform_inline(workers, job)
  match result {
    @worker.PerformResult::Discard(_) => ()
    _ => abort("expected Discard")
  }
}

///|
async test "drain_queue: runs all available jobs" {
  let eng = SandboxEngine::new()
  let _ = eng.insert_job(@job.Job::new("q", "W"))
  let _ = eng.insert_job(@job.Job::new("q", "W"))
  let workers : Map[
    String,
    async (@job.Job) -> @worker.PerformResult raise Error,
  ] = Map([("W", fn(_job) { @worker.PerformResult::Ok })])
  let count = drain_queue(eng, workers, "q")
  assert_eq(count, 2)
  assert_eq(eng.jobs_in_state(@job.Completed).length(), 2)
}

///|
async test "assert_enqueued_where: finds job by predicate" {
  let eng = SandboxEngine::new()
  let _ = eng.insert_job(@job.Job::new("q", "W").with_args("{\"x\":1}"))
  let found = assert_enqueued_where(eng, "q", fn(j) { j.args == "{\"x\":1}" })
  assert_eq(found, true)
  let not_found = assert_enqueued_where(eng, "q", fn(j) {
    j.args == "{\"x\":2}"
  })
  assert_eq(not_found, false)
}
