# worker

`PerformResult` — the return type of every tide worker function — and the
`Worker` open trait for struct-based workers.

## PerformResult

A worker function must return one of three variants:

```mbt check
///|
test {
  // Mark the job as completed
  let ok = @worker.PerformResult::Ok
  // Reschedule the job 30 seconds from now (attempt count unchanged)
  let snooze = @worker.PerformResult::Snooze(30)
  // Immediately move the job to the discarded state
  let discard = @worker.PerformResult::Discard("file not found")

  inspect(ok, content="Ok")
  inspect(snooze, content="Snooze(30)")
  inspect(discard, content="Discard(\"file not found\")")
}
```

If a worker function raises an error or times out, tide retries the job with
exponential backoff until `max_attempts` is exhausted, then discards it.

## Worker trait

For workers that need injected dependencies, implement the `Worker` open trait:

```mbt nocheck
///|
pub struct EmailSender {
  smtp_host : String
}

///|
pub impl @worker.Worker for EmailSender with fn perform(self, job) {
  send_email(self.smtp_host, job.args)
  @worker.PerformResult::Ok
}

///|
fn timeout_ms(_self) {
  10000
}
```

For simple workers without state, prefer the closure-based
`Instance::register` API instead.
