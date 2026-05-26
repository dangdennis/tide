# testing

In-memory test helpers for tide — unit-test workers and job pipelines without
a running Redis instance.

## SandboxEngine

`SandboxEngine` is a fully in-memory implementation of the `Engine` trait.
Insert jobs, run workers, and assert state — all without Redis:

```mbt check
///|
async test {
  let eng = @testing.SandboxEngine::new()

  // Insert a job
  let _ = eng.insert_job(@job.Job::new("default", "MyWorker"))

  // Check it landed in the queue
  inspect(@testing.assert_enqueued(eng, "default", "MyWorker"), content="true")
  inspect(eng.job_count(), content="1")
}
```

## drain_queue

Run all available jobs in a queue to completion:

```mbt check
///|
async test {
  let eng = @testing.SandboxEngine::new()
  let _ = eng.insert_job(@job.Job::new("default", "MyWorker"))
  let _ = eng.insert_job(@job.Job::new("default", "MyWorker"))

  let workers : Map[
    String,
    async (@job.Job) -> @worker.PerformResult raise Error,
  ] = { "MyWorker": async fn(_job) { @worker.PerformResult::Ok } }

  let count = @testing.drain_queue(eng, workers, "default")
  inspect(count, content="2")
  inspect(eng.jobs_in_state(@job.Completed).length(), content="2")
}
```

## perform_inline

Call a worker function directly, bypassing all queue machinery:

```mbt check
///|
async test {
  let workers : Map[
    String,
    async (@job.Job) -> @worker.PerformResult raise Error,
  ] = { "EchoWorker": async fn(_job) { @worker.PerformResult::Ok } }
  let job = @job.Job::new("default", "EchoWorker")
  let result = @testing.perform_inline(workers, job)
  match result {
    @worker.Ok => inspect("ok", content="ok")
    _ => inspect("unexpected", content="ok")
  }
}
```

## assert_enqueued_where

Filter by predicate when worker name alone isn't enough:

```mbt check
///|
async test {
  let eng = @testing.SandboxEngine::new()
  let _ = eng.insert_job(
    @job.Job::new("default", "ChargeWorker").with_args("{\"amount\":100}"),
  )

  let found = @testing.assert_enqueued_where(eng, "default", fn(j) {
    j.args == "{\"amount\":100}"
  })
  inspect(found, content="true")
}
```
