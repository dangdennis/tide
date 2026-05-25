// Public API surface for tide.

///|
pub(all) struct Config {
  redis_url : String
  queues : Map[String, Int] // queue name → concurrency limit
  pool_size : Int
  name : String // logical name; used for leader election key
  workers : Map[String, async (@job.Job) -> @worker.PerformResult raise Error]
  shutdown_grace_ms : Int // max wait for in-flight jobs on stop (default 15s)
  // Phase 3 plugin settings:
  prune_max_age_ms : Int64? // None = disabled; Some(n) = prune terminal jobs older than n ms
  lifeline_rescue_after_ms : Int64 // reclaim PEL entries idle > this (default 30 s)
  cron_entries : Array[@plugin.CronEntry] // zero or more cron-scheduled jobs
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
    prune_max_age_ms: None,
    lifeline_rescue_after_ms: 30000L,
    cron_entries: [],
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

// Run all queue loops and maintenance plugins.  Blocks until
// Instance::stop() is called and all in-flight jobs finish.

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
      // ── Fetch loop ──────────────────────────────────────────────────────
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
      // ── Stager: scheduled → available ──────────────────────────────────
      let q = queue
      tg.spawn_loop(no_wait=true, allow_failure=true) <| () => {
        if self.shutdown.is_stopped() {
          raise @async.BreakFromSpawnLoop
        }
        @plugin.stager_tick(self.engine, q)
      }
      // ── Lifeline: reclaim stale PEL entries ────────────────────────────
      let rescue_ms = self.config.lifeline_rescue_after_ms
      tg.spawn_loop(no_wait=true, allow_failure=true) <| () => {
        if self.shutdown.is_stopped() {
          raise @async.BreakFromSpawnLoop
        }
        @plugin.lifeline_tick(self.engine, q, rescue_ms)
      }
      // ── Pruner: delete old terminal jobs (optional) ─────────────────────
      match self.config.prune_max_age_ms {
        None => ()
        Some(max_age_ms) => {
          let age = max_age_ms
          tg.spawn_loop(no_wait=true, allow_failure=true) <| () => {
            if self.shutdown.is_stopped() {
              raise @async.BreakFromSpawnLoop
            }
            @plugin.pruner_tick(self.engine, q, age, 60000)
          }
        }
      }
    }
    // ── Cron: per-minute job scheduler ─────────────────────────────────────
    if !self.config.cron_entries.is_empty() {
      let entries = self.config.cron_entries
      let last_minute : Ref[Int64] = Ref(-1L)
      tg.spawn_loop(no_wait=true, allow_failure=true) <| () => {
        if self.shutdown.is_stopped() {
          raise @async.BreakFromSpawnLoop
        }
        let now = @async.now()
        let current_minute = now / 60000L
        if current_minute != last_minute.val {
          last_minute.val = current_minute
          let tick_ms = current_minute * 60000L
          for entry in entries {
            let _ = @plugin.cron_fire(self.engine, entry, tick_ms) catch {
              _ => false
            }
          }
        }
        @async.sleep(1000)
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
