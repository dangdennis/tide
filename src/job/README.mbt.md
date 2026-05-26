# job

Core job types for tide — `Job`, `JobState`, priority constants, unique
deduplication config, tag helpers, and flat-JSON meta helpers.

## Building a job

`Job::new(queue, worker)` returns a job with sensible defaults.  Chain builder
methods to set options:

```mbt check
///|
test {
  let job = @job.Job::new("emails", "WelcomeEmailWorker")
    .with_args("{\"user_id\":42}")
    .with_max_attempts(5)
    .with_priority(@job.PRIORITY_HIGH)
    .with_tags(["onboarding", "email"])
  inspect(job.queue, content="emails")
  inspect(job.worker, content="WelcomeEmailWorker")
  inspect(job.max_attempts, content="5")
  inspect(job.priority, content="2")
  inspect(job.tags.length(), content="2")
}
```

## Scheduling in the future

```mbt check
///|
test {
  // Schedule 60 seconds from now (epoch ms)
  let now_ms = 1_000_000L
  let job = @job.Job::new("default", "ReminderWorker").with_scheduled_at(
    now_ms + 60_000L,
  )
  inspect(job.scheduled_at, content="1060000")
}
```

## Priority constants

```mbt check
///|
test {
  inspect(@job.PRIORITY_CRITICAL, content="0")
  inspect(@job.PRIORITY_HIGH, content="2")
  inspect(@job.PRIORITY_NORMAL, content="5")
  inspect(@job.PRIORITY_LOW, content="7")
  inspect(@job.PRIORITY_LOWEST, content="9")
}
```

## Unique jobs

`UniqueConfig` controls deduplication within a sliding time window:

```mbt check
///|
test {
  let job = @job.Job::new("default", "DailyReportWorker")
  let ucfg = @job.UniqueConfig::new(3_600_000L) // 1 hour
  let key = @job.compute_unique_key(job, ucfg)
  // Key is deterministic for same queue + worker + args
  inspect(key.is_empty(), content="false")
}
```

## Tag helpers

```mbt check
///|
test {
  let job = @job.Job::new("default", "MyWorker").with_tags([
    "billing", "user:42",
  ])
  inspect(@job.job_has_tag(job, "billing"), content="true")
  inspect(@job.job_has_tag(job, "missing"), content="false")

  let jobs = [job]
  inspect(@job.jobs_with_tag(jobs, "user:42").length(), content="1")
}
```

## Meta helpers

`meta` is a flat JSON object attached to a job for plugin metadata:

```mbt check
///|
test {
  let m = @job.meta_set_string("{}", "source", "api")
  let m2 = @job.meta_set_bool(m, "retried", true)
  inspect(@job.meta_get_string(m2, "source").unwrap(), content="api")
  inspect(@job.meta_get_bool(m2, "retried").unwrap(), content="true")
  inspect(@job.meta_get_string(m2, "missing").unwrap_or(""), content="")
  inspect(
    @job.meta_get_string(m2, "source").unwrap_or("missing"),
    content="api",
  )
}
```
