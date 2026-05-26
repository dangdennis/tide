# tide

The main entry point for the tide job queue library.  Create a `Config`,
start an `Instance`, register workers, and call `run()` to begin processing.

## Config

`Config::default()` returns a sensible starting point.  Spread it to override
only the fields you care about:

```mbt check
///|
test {
  let config = {
    ..@tide.Config::default(),
    redis_url: "redis://127.0.0.1:6379",
  }
  inspect(config.name, content="tide")
  inspect(config.pool_size, content="5")
  inspect(config.shutdown_grace_ms, content="15000")
  inspect(config.job_timeout_ms, content="30000")
}
```

## Complete lifecycle

```mbt nocheck
// 1. Build config
let config = {
  ..@tide.Config::default(),
  redis_url: "redis://prod-redis:6379",
  queues: Map([("critical", 20), ("default", 10), ("bulk", 5)]),
  dashboard_port: Some(4567),
}

// 2. Start instance (connects to Redis, creates stream groups)
let instance = @tide.Instance::start(config)

// 3. Register worker functions
instance.register("SendEmailWorker", async fn(job) {
  println("sending email to: \{job.args}")
  @worker.PerformResult::Ok
})

// 4. Insert a job
let job = @job.Job::new("default", "SendEmailWorker")
  .with_args("{\"to\":\"user@example.com\"}")
instance.insert(job)

// 5. Block until instance.stop() is called
instance.run()
```

## Graceful shutdown

```mbt nocheck
// Signal all queue loops to drain
instance.stop()
// Instance::run() returns once all in-flight jobs finish
// (or after shutdown_grace_ms, default 15 s)
```

## Multi-node mode

```mbt nocheck
let config = {
  ..@tide.Config::default(),
  name: "my-app",
  peer_mode: Redis,    // leader election via SET NX PX
  notify_cancel: true, // broadcast cancel signals via pub/sub
}
let instance = @tide.Instance::start(config)
instance.is_leader() // true on the current leader node
```
