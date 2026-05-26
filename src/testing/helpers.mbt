// Testing helpers for tide workers.
//
// These helpers work with SandboxEngine (no Redis required).
//
// perform_inline  — call a worker function directly, bypassing the engine
// assert_enqueued — check whether a job is waiting in a queue
// drain_queue     — run all available jobs in a queue to completion

// ── perform_inline ────────────────────────────────────────────────────────────

///|
/// Call a worker function directly against `job`, without touching any engine.
///
/// Returns the PerformResult or propagates any error the worker raises.
/// Useful for unit-testing a single worker in total isolation.
pub async fn perform_inline(
  workers : Map[String, async (@job.Job) -> @worker.PerformResult raise Error],
  job : @job.Job,
) -> @worker.PerformResult raise Error {
  match workers.get(job.worker) {
    None =>
      @worker.PerformResult::Discard("no worker registered for '\{job.worker}'")
    Some(f) => f(job)
  }
}

// ── assert_enqueued ───────────────────────────────────────────────────────────

///|
/// Return true if at least one available job in `queue` has `worker` as its
/// worker name.
pub fn assert_enqueued(
  engine : SandboxEngine,
  queue : String,
  worker : String,
) -> Bool {
  match engine.queues.get(queue) {
    None => false
    Some(ids) => {
      let mut found = false
      let mut i = 0
      while i < ids.length() {
        match engine.jobs.get(ids[i]) {
          Some(j) if j.worker == worker => {
            found = true
            break
          }
          _ => ()
        }
        i = i + 1
      }
      found
    }
  }
}

///|
/// Return true if at least one available job in `queue` satisfies `pred`.
pub fn assert_enqueued_where(
  engine : SandboxEngine,
  queue : String,
  pred : (@job.Job) -> Bool,
) -> Bool {
  match engine.queues.get(queue) {
    None => false
    Some(ids) => {
      let mut found = false
      let mut i = 0
      while i < ids.length() {
        match engine.jobs.get(ids[i]) {
          Some(j) if pred(j) => {
            found = true
            break
          }
          _ => ()
        }
        i = i + 1
      }
      found
    }
  }
}

// ── drain_queue ───────────────────────────────────────────────────────────────

///|
/// Execute every available job in `queue` against `workers`, exactly once.
///
/// Each job is fetched, the worker is called inline, and the result is
/// reflected back into the engine (complete / snooze / discard / error).
/// Returns the total number of jobs processed.
///
/// Note: jobs that are re-enqueued (retry / snooze) are NOT re-processed in
/// the same drain call — drain iterates only the jobs available at call time.
pub async fn drain_queue(
  engine : SandboxEngine,
  workers : Map[String, async (@job.Job) -> @worker.PerformResult raise Error],
  queue : String,
) -> Int raise @engine.EngineError {
  let fetched = engine.fetch_jobs(queue~, limit=10000)
  let mut count = 0
  for f in fetched {
    let result = try? perform_inline(workers, f.job)
    match result {
      Ok(@worker.PerformResult::Ok) => engine.complete_job(f)
      Ok(@worker.PerformResult::Snooze(secs)) =>
        engine.snooze_job(
          f,
          scheduled_at=@async.now() + Int64::from_int(secs) * 1000L,
        )
      Ok(@worker.PerformResult::Discard(_reason)) => engine.discard_job(f)
      Err(e) => engine.error_job(f, error_msg="\{e}", next_scheduled_at=None)
    }
    count = count + 1
  }
  count
}
