// Unit tests for telemetry payload builder functions and event variants.

///|
test "job_start_payload sets duration_ms to 0 and error to empty" {
  let job = @job.Job::new("q", "w")
  let p = job_start_payload(job, 1000L)
  assert_eq(p.started_at, 1000L)
  assert_eq(p.duration_ms, 0L)
  assert_eq(p.error, "")
}

///|
test "job_stop_payload computes elapsed duration_ms" {
  let job = @job.Job::new("q", "w")
  let start = job_start_payload(job, 1000L)
  let stop = job_stop_payload(start, 1250L)
  assert_eq(stop.duration_ms, 250L)
  assert_eq(stop.error, "")
}

///|
test "job_exception_payload records duration_ms and error message" {
  let job = @job.Job::new("q", "w")
  let start = job_start_payload(job, 500L)
  let exc = job_exception_payload(start, 750L, "timeout")
  assert_eq(exc.duration_ms, 250L)
  assert_eq(exc.error, "timeout")
}

///|
test "job_stop_payload preserves started_at from start payload" {
  let job = @job.Job::new("q", "w")
  let start = job_start_payload(job, 9000L)
  let stop = job_stop_payload(start, 9100L)
  assert_eq(stop.started_at, 9000L)
}

///|
test "TelemetryEvent variants wrap a JobEventPayload" {
  let job = @job.Job::new("q", "w")
  let p = job_start_payload(job, 0L)
  let e1 = TelemetryEvent::JobStart(p)
  let e2 = TelemetryEvent::JobStop(p)
  let e3 = TelemetryEvent::JobException(p)
  match e1 {
    JobStart(_) => ()
    _ => assert_true(false)
  }
  match e2 {
    JobStop(_) => ()
    _ => assert_true(false)
  }
  match e3 {
    JobException(_) => ()
    _ => assert_true(false)
  }
}
