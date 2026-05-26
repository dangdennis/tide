# telemetry

Observe job lifecycle events — start, completion, and exceptions — by
implementing the `Handler` open trait.

## Handler trait

```mbt nocheck
///|
pub struct MyHandler {
  logger : Logger
}

///|
pub impl @telemetry.Handler for MyHandler with fn handle(self, event) {
  match event {
    JobStart(p) =>
      self.logger.info("job started", job_id=p.job.id, at=p.started_at)
    JobStop(p) =>
      self.logger.info("job completed", job_id=p.job.id, ms=p.duration_ms)
    JobException(p) =>
      self.logger.error("job failed", job_id=p.job.id, err=p.error)
  }
}
```

## JobEventPayload fields

| Field | Type | Description |
|-------|------|-------------|
| `job` | `@job.Job` | The job that was executed |
| `started_at` | `Int64` | Epoch ms when the job started |
| `duration_ms` | `Int64` | Wall-clock time from start to stop/exception |
| `error` | `String` | Non-empty only for `JobException` events |

## Events

```mbt check
///|
test {
  // TelemetryEvent variants
  let job = @job.Job::new("default", "MyWorker")
  let payload = @telemetry.job_start_payload(job, 1_000_000L)
  let stopped = @telemetry.job_stop_payload(payload, 1_000_250L)
  inspect(stopped.duration_ms, content="250")
  inspect(stopped.error, content="")

  let failed = @telemetry.job_exception_payload(payload, 1_000_100L, "timeout")
  inspect(failed.error, content="timeout")
  inspect(failed.duration_ms, content="100")
}
```
