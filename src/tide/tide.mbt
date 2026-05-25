// Public API surface for tide.

///|
pub struct Config {
  redis_url : String
  queues : Map[String, Int] // queue name → concurrency limit
  pool_size : Int
  name : String // logical name; used for leader election key
  workers : Map[String, async (@job.Job) -> @worker.PerformResult raise Error]
  shutdown_grace_ms : Int // max wait for in-flight jobs on stop (default 15s)
}

///|
pub fn Config::default() -> Config {
  {
    redis_url: "redis://127.0.0.1:6379",
    queues: Map([("default", 10)]),
    pool_size: 5,
    name: "tide",
    workers: Map([]),
    shutdown_grace_ms: 15000,
  }
}

// Tide instance — owns the engine and the shutdown signal.

///|
pub struct Instance {
  engine : @engine.RedisEngine
  config : Config
  shutdown : @queue.ShutdownSignal
}

///|
pub async fn Instance::start(config : Config) -> Instance raise TideError {
  let redis_config = @redis.ClientConfig::from_url(config.redis_url) catch {
    e => raise TideError::ConfigError("\{e}")
  }
  let engine = @engine.RedisEngine::new(redis_config, config.pool_size) catch {
    e => raise TideError::EngineError("\{e}")
  }
  for queue, _ in config.queues {
    engine.ensure_stream_group(queue~) catch {
      e => raise TideError::EngineError("\{e}")
    }
  }
  { engine, config, shutdown: @queue.ShutdownSignal::new() }
}

// Register a worker function under a name that matches Job::worker.

///|
pub fn Instance::register(
  self : Instance,
  name : String,
  f : async (@job.Job) -> @worker.PerformResult raise Error,
) -> Unit {
  self.config.workers[name] = f
}

// Insert a single job. Returns the persisted job with id/timestamps populated.

///|
pub async fn Instance::insert(
  self : Instance,
  job : @job.Job,
) -> @job.Job raise TideError {
  self.engine.insert_job(job) catch {
    e => raise TideError::EngineError("\{e}")
  }
}

// Insert multiple jobs (not transactional across jobs).

///|
pub async fn Instance::insert_all(
  self : Instance,
  jobs : Array[@job.Job],
) -> Array[@job.Job] raise TideError {
  self.engine.insert_all(jobs) catch {
    e => raise TideError::EngineError("\{e}")
  }
}

// Run all queue loops. Blocks until Instance::stop() is called and all
// in-flight jobs finish (or the shutdown grace period expires).

///|
pub async fn Instance::run(self : Instance) -> Unit raise TideError {
  @async.with_task_group(async fn(tg) {
    for queue, concurrency in self.config.queues {
      let runner = @queue.QueueRunner::new(
        self.engine,
        self.config.workers,
        queue,
        concurrency,
        self.shutdown,
      )
      tg.spawn_loop(no_wait=true, allow_failure=true) <| () => {
        if runner.shutdown.is_stopped() {
          raise @async.BreakFromSpawnLoop
        }
        let jobs = runner.fetch_batch()
        for fetched in jobs {
          let r = runner
          tg.spawn_bg(allow_failure=true) <| () => {
            @queue.execute(r.engine, r.workers, fetched) catch {
              _ => ()
            }
          }
        }
      }
    }
    self.shutdown.wait() catch {
      _ => ()
    }
  }) catch {
    e => raise TideError::EngineError("\{e}")
  }
}

// Signal all queue loops to stop fetching and begin graceful drain.

///|
pub fn Instance::stop(self : Instance) -> Unit {
  self.shutdown.send()
}

///|
pub suberror TideError {
  ConfigError(String)
  EngineError(String)
}

///|
pub impl Show for TideError with fn output(self, logger) {
  match self {
    ConfigError(msg) => logger.write_string("ConfigError(\{msg})")
    EngineError(msg) => logger.write_string("EngineError(\{msg})")
  }
}
