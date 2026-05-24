// Public API surface for tide.
// Phase 1: insert only. Queue loops and full runtime ship in Phase 2.

///|
pub struct Config {
  redis_url : String
  queues : Map[String, Int] // queue name → concurrency limit
  pool_size : Int
  name : String // logical name; used for leader election key
}

///|
pub fn Config::default() -> Config {
  {
    redis_url: "redis://127.0.0.1:6379",
    queues: Map([("default", 10)]),
    pool_size: 5,
    name: "tide",
  }
}

// Tide instance — owns the engine and (in Phase 2) the queue loop tasks.

///|
pub struct Instance {
  engine : @engine.RedisEngine
  config : Config
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
  { engine, config }
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
