///|
/// The value a worker function returns to tell tide what to do with the job.
/// `Ok` marks it completed; `Snooze(n)` reschedules it by `n` seconds;
/// `Discard(reason)` moves it to the discarded state immediately.
pub(all) enum PerformResult {
  Ok
  Snooze(Int) // snooze for N seconds before retrying
  Discard(String) // discard the job with a reason
} derive(Debug)

///|
pub impl Show for PerformResult with fn output(self, logger) {
  match self {
    Ok => logger.write_string("Ok")
    Snooze(n) => logger.write_string("Snooze(\{n})")
    Discard(reason) => logger.write_string("Discard(\"\{reason}\")")
  }
}

///|
/// Open trait for implementing a typed worker.
/// Prefer the closure-based `Instance::register` API for simple workers;
/// implement this trait when you need a struct with injected dependencies.
pub trait Worker {
  fn perform(Self, job : @job.Job) -> PerformResult raise Error
  fn timeout_ms(Self) -> Int
}
