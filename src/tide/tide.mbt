// Public API surface for tide.

///|
pub(all) enum PeerMode {
  Local // single-node; always the leader, no Redis election
  Redis // multi-node; leader election via SET NX PX
} derive(Debug)

///|
pub(all) struct Config {
  redis_url : String
  queues : Map[String, Int] // queue name → concurrency limit
  pool_size : Int
  name : String // logical name; used for leader election key
  workers : Map[String, async (@job.Job) -> @worker.PerformResult raise Error]
  shutdown_grace_ms : Int // max wait for in-flight jobs on stop (default 15s)
  job_timeout_ms : Int // per-job execution timeout (default 30 s)
  // Phase 3 plugin settings:
  prune_max_age_ms : Int64? // None = disabled; Some(n) = prune terminal jobs older than n ms
  lifeline_rescue_after_ms : Int64 // reclaim PEL entries idle > this (default 30 s)
  cron_entries : Array[@plugin.CronEntry] // zero or more cron-scheduled jobs
  // Phase 5 multi-node coordination:
  peer_mode : PeerMode // Local (default) or Redis for multi-node election
  notify_cancel : Bool // true = enable pub/sub cancel-signal propagation
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
    job_timeout_ms: 30000,
    prune_max_age_ms: None,
    lifeline_rescue_after_ms: 30000L,
    cron_entries: [],
    peer_mode: Local,
    notify_cancel: false,
  }
}

// Tide instance — owns the engine and the shutdown signal.

///|
pub struct Instance {
  engine : @engine.RedisEngine
  config : Config
  shutdown : @queue.ShutdownSignal
  peer : @peer.RedisPeer? // None when peer_mode = Local
  notifier : @notifier.RedisNotifier? // None when notify_cancel = false
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
  let peer : @peer.RedisPeer? = match config.peer_mode {
    Local => None
    Redis =>
      Some(@peer.RedisPeer::new(engine.pool, config.name, engine.node_id))
  }
  let notifier : @notifier.RedisNotifier? = if config.notify_cancel {
    Some(
      @notifier.RedisNotifier::new(
        redis_config,
        "tide:notify:\{config.name}",
      ),
    )
  } else {
    None
  }
  { engine, config, shutdown: @queue.ShutdownSignal::new(), peer, notifier }
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
        self.config.job_timeout_ms,
        self.shutdown,
      )
      // ── Fetch loop ──────────────────────────────────────────────────────
      tg.spawn_loop(no_wait=true, allow_failure=true) <| () => {
        if runner.shutdown.is_stopped() {
          raise @async.BreakFromSpawnLoop
        }
        // Fetch up to concurrency jobs.  XREADGROUP blocks up to 1 s if the
        // stream is empty, giving in-flight jobs time to complete and release
        // semaphore slots before the next iteration.
        let jobs = runner.engine.fetch_jobs(
          queue=runner.queue,
          limit=runner.concurrency,
        )
        for fetched in jobs {
          let r = runner
          match self.notifier {
            Some(n) if n.is_cancelled(fetched.job.id) => continue
            _ => ()
          }
          // Acquire one slot before dispatching.  Blocks (yields) when
          // concurrency is saturated, letting completed tasks release first.
          r.sem.acquire()
          tg.spawn_bg(allow_failure=true) <| () => {
            defer { r.sem.release() }
            @queue.execute(r.engine, r.workers, fetched, timeout_ms=r.timeout_ms) catch {
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
    // ── Peer: leader election renewal ──────────────────────────────────────
    match self.peer {
      None => ()
      Some(p) => {
        let stopped = self.shutdown
        tg.spawn_bg(allow_failure=true) <| () => {
          p.run(fn() { stopped.is_stopped() })
        }
      }
    }
    // ── Notifier: cancel-signal listener ───────────────────────────────────
    match self.notifier {
      None => ()
      Some(n) => {
        let stopped = self.shutdown
        tg.spawn_bg(allow_failure=true) <| () => {
          n.listen(fn() { stopped.is_stopped() })
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

// Insert a job only if no other job with the same uniqueness key currently
// exists in Redis.  The key is derived from `job` and `config` via
// @job.compute_unique_key and the lock expires after `config.period_ms`.

///|
pub async fn Instance::insert_unique(
  self : Instance,
  job : @job.Job,
  config : @job.UniqueConfig,
) -> @engine.UniqueInsertResult raise TideError {
  let unique_key = @job.compute_unique_key(job, config)
  self.engine.insert_unique_job(
    job,
    unique_key~,
    period_ms=config.period_ms,
  ) catch {
    e => raise TideError::EngineError("\{e}")
  }
}

// Cancel all non-terminal jobs in `queue` that carry `tag`.
// Also publishes per-job cancel signals when notify_cancel is enabled,
// so other nodes' pre-dispatch checks can skip already-cancelled jobs.

///|
pub async fn Instance::cancel_by_tag(
  self : Instance,
  queue : String,
  tag : String,
) -> Int raise TideError {
  let ids = self.engine.cancel_jobs_by_tag(queue~, tag~) catch {
    e => raise TideError::EngineError("\{e}")
  }
  match self.notifier {
    Some(n) =>
      for id in ids {
        n.publish_cancel(self.engine.pool, id) catch { _ => () }
      }
    None => ()
  }
  ids.length()
}

// Re-enqueue discarded or cancelled jobs in `queue` that carry `tag`.

///|
pub async fn Instance::retry_by_tag(
  self : Instance,
  queue : String,
  tag : String,
) -> Int raise TideError {
  self.engine.retry_jobs_by_tag(queue~, tag~) catch {
    e => raise TideError::EngineError("\{e}")
  }
}

// Returns true if this node is the current leader.
// Always true when peer_mode = Local; reflects Redis election otherwise.

///|
pub fn Instance::is_leader(self : Instance) -> Bool {
  match self.peer {
    None => true
    Some(p) => p.is_leader()
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
