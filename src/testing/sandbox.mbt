// SandboxEngine — an in-memory Engine implementation for unit tests.
//
// No Redis required. Jobs are stored in a plain Map; the available queue is an
// ordered Array per queue name.  Stream IDs are synthesised from job IDs.
//
// Usage:
//   let eng = SandboxEngine::new()
//   let job = @job.Job::new("default", "MyWorker")
//   let inserted = eng.insert_job(job)
//   let fetched  = eng.fetch_jobs(queue="default", limit=10)

// ── Data structures ───────────────────────────────────────────────────────────

///|
/// In-memory engine for unit tests.
pub struct SandboxEngine {
  /// All jobs by id (every state).
  jobs : Map[String, @job.Job]
  /// Per-queue list of *available* job ids (FIFO order).
  queues : Map[String, Array[String]]
  /// Active unique-key lock → expiry epoch ms.
  unique_keys : Map[String, Int64]
  /// Monotonic counter for id generation.
  counter : Ref[Int]
}

///|
pub fn SandboxEngine::new() -> SandboxEngine {
  { jobs: Map([]), queues: Map([]), unique_keys: Map([]), counter: Ref(0) }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

///|
fn SandboxEngine::next_id(self : SandboxEngine) -> String {
  self.counter.val = self.counter.val + 1
  "sandbox_\{self.counter.val}"
}

///|
/// Retrieve (or lazily create) the available-job list for a queue.
fn SandboxEngine::queue_list(
  self : SandboxEngine,
  queue : String,
) -> Array[String] {
  match self.queues.get(queue) {
    Some(arr) => arr
    None => {
      let arr : Array[String] = []
      self.queues[queue] = arr
      arr
    }
  }
}

///|
/// Remove a job id from the available list (if present).
fn SandboxEngine::dequeue(
  self : SandboxEngine,
  queue : String,
  id : String,
) -> Unit {
  match self.queues.get(queue) {
    None => ()
    Some(arr) => {
      let mut i = 0
      while i < arr.length() {
        if arr[i] == id {
          let _ = arr.remove(i)
          break
        }
        i = i + 1
      }
    }
  }
}

///|
fn make_inserted(job : @job.Job, id : String, now : Int64) -> @job.Job {
  let scheduled_at = if job.scheduled_at > 0L { job.scheduled_at } else { now }
  { ..job, id, inserted_at: now, scheduled_at }
}

// ── Engine trait implementation ───────────────────────────────────────────────

///|
pub impl @engine.Engine for SandboxEngine with fn insert_job(self, job) -> @job.Job raise @engine.EngineError {
  let id = self.next_id()
  let now = @async.now()
  let inserted = make_inserted(job, id, now)
  self.jobs[id] = inserted
  if inserted.scheduled_at <= now {
    self.queue_list(job.queue).push(id)
  }
  inserted
}

///|
pub impl @engine.Engine for SandboxEngine with fn insert_all(self, jobs) -> Array[
  @job.Job,
] raise @engine.EngineError {
  let result : Array[@job.Job] = []
  for j in jobs {
    result.push(self.insert_job(j))
  }
  result
}

///|
pub impl @engine.Engine for SandboxEngine with fn fetch_jobs(
  self,
  queue~ : String,
  limit~ : Int,
) -> Array[@job.FetchedJob] raise @engine.EngineError {
  let available = self.queue_list(queue)
  let take = if available.length() < limit { available.length() } else { limit }
  let result : Array[@job.FetchedJob] = []
  let mut i = 0
  while i < take {
    let id = available[0]
    let _ = available.remove(0)
    match self.jobs.get(id) {
      None => ()
      Some(job) => {
        let now = @async.now()
        let updated = { ..job, state: @job.Executing, attempted_at: Some(now) }
        self.jobs[id] = updated
        result.push(@job.FetchedJob::new(updated, id))
      }
    }
    i = i + 1
  }
  result
}

///|
pub impl @engine.Engine for SandboxEngine with fn complete_job(self, fetched) -> Unit raise @engine.EngineError {
  let id = fetched.job.id
  match self.jobs.get(id) {
    None => ()
    Some(job) =>
      self.jobs[id] = {
        ..job,
        state: @job.Completed,
        completed_at: Some(@async.now()),
      }
  }
}

///|
pub impl @engine.Engine for SandboxEngine with fn error_job(
  self,
  fetched,
  error_msg~ : String,
  next_scheduled_at~ : Int64?,
) -> Unit raise @engine.EngineError {
  let id = fetched.job.id
  match self.jobs.get(id) {
    None => ()
    Some(job) => {
      let now = @async.now()
      let new_attempt = job.attempt + 1
      let new_errors = {
        let e = job.errors.copy()
        e.push(error_msg)
        e
      }
      match next_scheduled_at {
        Some(scheduled_at) => {
          let updated = {
            ..job,
            state: @job.Retryable,
            attempt: new_attempt,
            errors: new_errors,
            scheduled_at,
          }
          self.jobs[id] = updated
          // Re-enqueue for sandbox (ignore scheduled_at timing).
          self.queue_list(job.queue).push(id)
        }
        None =>
          self.jobs[id] = {
            ..job,
            state: @job.Discarded,
            attempt: new_attempt,
            errors: new_errors,
            discarded_at: Some(now),
          }
      }
    }
  }
}

///|
pub impl @engine.Engine for SandboxEngine with fn snooze_job(
  self,
  fetched,
  scheduled_at~ : Int64,
) -> Unit raise @engine.EngineError {
  let id = fetched.job.id
  match self.jobs.get(id) {
    None => ()
    Some(job) => {
      self.jobs[id] = { ..job, state: @job.Scheduled, scheduled_at }
      // Re-add to available for sandbox simplicity.
      self.queue_list(job.queue).push(id)
    }
  }
}

///|
pub impl @engine.Engine for SandboxEngine with fn discard_job(self, fetched) -> Unit raise @engine.EngineError {
  let id = fetched.job.id
  match self.jobs.get(id) {
    None => ()
    Some(job) =>
      self.jobs[id] = {
        ..job,
        state: @job.Discarded,
        discarded_at: Some(@async.now()),
      }
  }
}

///|
pub impl @engine.Engine for SandboxEngine with fn cancel_job(self, id~ : String) -> Unit raise @engine.EngineError {
  match self.jobs.get(id) {
    None => ()
    Some(job) => {
      self.jobs[id] = {
        ..job,
        state: @job.Cancelled,
        cancelled_at: Some(@async.now()),
      }
      self.dequeue(job.queue, id)
    }
  }
}

///|
pub impl @engine.Engine for SandboxEngine with fn retry_job(self, id~ : String) -> Unit raise @engine.EngineError {
  match self.jobs.get(id) {
    None => ()
    Some(job) => {
      self.jobs[id] = {
        ..job,
        state: @job.Available,
        discarded_at: None,
        cancelled_at: None,
      }
      self.queue_list(job.queue).push(id)
    }
  }
}

///|
pub impl @engine.Engine for SandboxEngine with fn prune_jobs(
  self,
  queue~ : String,
  older_than_ms~ : Int64,
) -> Int raise @engine.EngineError {
  let cutoff = @async.now() - older_than_ms
  let to_delete : Array[String] = []
  for id, job in self.jobs {
    if job.queue == queue && is_terminal(job.state) && job.inserted_at < cutoff {
      to_delete.push(id)
    }
  }
  for id in to_delete {
    self.jobs.remove(id)
  }
  to_delete.length()
}

///|
pub impl @engine.Engine for SandboxEngine with fn ensure_stream_group(
  self,
  queue~ : String,
) -> Unit raise @engine.EngineError {
  // No-op for sandbox — just ensure the queue list exists.
  let _ = self.queue_list(queue)
}

///|
pub impl @engine.Engine for SandboxEngine with fn ensure_scripts(_self) -> Unit raise @engine.EngineError {
  // No-op for sandbox.
}

///|
pub impl @engine.Engine for SandboxEngine with fn insert_unique_job(
  self,
  job,
  unique_key~ : String,
  period_ms~ : Int64,
) -> @engine.UniqueInsertResult raise @engine.EngineError {
  let now = @async.now()
  match self.unique_keys.get(unique_key) {
    Some(expiry) if expiry > now =>
      return @engine.UniqueInsertResult::AlreadyExists
    _ => ()
  }
  let inserted = self.insert_job(job)
  self.unique_keys[unique_key] = now + period_ms
  @engine.UniqueInsertResult::Inserted(inserted)
}

///|
pub impl @engine.Engine for SandboxEngine with fn cancel_jobs_by_tag(
  self,
  queue~ : String,
  tag~ : String,
) -> Array[String] raise @engine.EngineError {
  let now = @async.now()
  // Collect matching ids first to avoid mutating during iteration.
  let to_cancel : Array[(String, @job.Job)] = []
  for id, job in self.jobs {
    if job.queue == queue && !is_terminal(job.state) && job.tags.contains(tag) {
      to_cancel.push((id, job))
    }
  }
  let cancelled : Array[String] = []
  let mut ci = 0
  while ci < to_cancel.length() {
    let (id, job) = to_cancel[ci]
    self.jobs[id] = { ..job, state: @job.Cancelled, cancelled_at: Some(now) }
    self.dequeue(queue, id)
    cancelled.push(id)
    ci = ci + 1
  }
  cancelled
}

///|
pub impl @engine.Engine for SandboxEngine with fn retry_jobs_by_tag(
  self,
  queue~ : String,
  tag~ : String,
) -> Int raise @engine.EngineError {
  // Collect matching ids first to avoid mutating during iteration.
  let to_retry : Array[(String, @job.Job)] = []
  for id, job in self.jobs {
    if job.queue == queue &&
      (job.state == @job.Discarded || job.state == @job.Cancelled) &&
      job.tags.contains(tag) {
      to_retry.push((id, job))
    }
  }
  let mut ri = 0
  while ri < to_retry.length() {
    let (id, job) = to_retry[ri]
    self.jobs[id] = {
      ..job,
      state: @job.Available,
      discarded_at: None,
      cancelled_at: None,
    }
    self.queue_list(queue).push(id)
    ri = ri + 1
  }
  to_retry.length()
}

// ── Inspection helpers ────────────────────────────────────────────────────────

///|
/// Return all jobs currently in `state` across all queues.
pub fn SandboxEngine::jobs_in_state(
  self : SandboxEngine,
  state : @job.JobState,
) -> Array[@job.Job] {
  let result : Array[@job.Job] = []
  for _, job in self.jobs {
    if job.state == state {
      result.push(job)
    }
  }
  result
}

///|
/// Return all jobs for a given queue in any state.
pub fn SandboxEngine::jobs_for_queue(
  self : SandboxEngine,
  queue : String,
) -> Array[@job.Job] {
  let result : Array[@job.Job] = []
  for _, job in self.jobs {
    if job.queue == queue {
      result.push(job)
    }
  }
  result
}

///|
/// Total number of jobs stored (all states).
pub fn SandboxEngine::job_count(self : SandboxEngine) -> Int {
  self.jobs.length()
}

// ── Private helpers ───────────────────────────────────────────────────────────

///|
fn is_terminal(state : @job.JobState) -> Bool {
  match state {
    @job.Completed | @job.Discarded | @job.Cancelled => true
    _ => false
  }
}
