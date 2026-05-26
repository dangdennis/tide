// Telemetry events emitted during job execution.
//
// Implement the Handler trait to observe job lifecycle events.
//
// Event flow:
//   JobStart → (worker runs) → JobStop
//                           ↘ JobException (on error or timeout)
//
// Usage:
//   pub struct LogHandler {}
//   pub impl @telemetry.Handler for LogHandler with handle(self, event) {
//     match event {
//       JobStart(p)     => println("[\{p.started_at}] start \{p.job.id}")
//       JobStop(p)      => println("done in \{p.duration_ms}ms")
//       JobException(p) => println("error: \{p.error}")
//     }
//   }

///|
/// Metadata attached to every telemetry event.
pub(all) struct JobEventPayload {
  /// The job being executed.
  job : @job.Job
  /// Unix milliseconds when execution of this job began.
  started_at : Int64
  /// Elapsed milliseconds since started_at.
  /// Zero for JobStart events.
  duration_ms : Int64
  /// Error or timeout message. Empty for non-exception events.
  error : String
} derive(Debug)

///|
pub(all) enum TelemetryEvent {
  /// Fired just before the worker function is invoked.
  JobStart(JobEventPayload)
  /// Fired after the worker function returns (Ok, Snooze, or Discard).
  JobStop(JobEventPayload)
  /// Fired when the worker function raises an error or the job times out.
  JobException(JobEventPayload)
} derive(Debug)

///|
/// Implement this trait to receive telemetry events from the tide executor.
///
/// The handler is called synchronously in the executor task — keep it fast.
/// Offload heavy work (HTTP, DB writes) to a channel or background task.
pub(open) trait Handler {
  fn handle(Self, TelemetryEvent) -> Unit
}

///|
/// Build a JobStart payload.
pub fn job_start_payload(job : @job.Job, started_at : Int64) -> JobEventPayload {
  { job, started_at, duration_ms: 0L, error: "" }
}

///|
/// Build a JobStop payload from a start payload.
pub fn job_stop_payload(
  payload : JobEventPayload,
  now : Int64,
) -> JobEventPayload {
  { ..payload, duration_ms: now - payload.started_at }
}

///|
/// Build a JobException payload from a start payload.
pub fn job_exception_payload(
  payload : JobEventPayload,
  now : Int64,
  error : String,
) -> JobEventPayload {
  { ..payload, duration_ms: now - payload.started_at, error }
}
