///|
pub(all) enum PerformResult {
  Ok
  Snooze(Int) // snooze for N seconds before retrying
  Discard(String) // discard the job with a reason
} derive(Debug)

///|
pub trait Worker {
  fn perform(Self, job : @job.Job) -> PerformResult raise Error
  fn timeout_ms(Self) -> Int
}
