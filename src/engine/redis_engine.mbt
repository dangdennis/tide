// Redis-backed Engine implementation.
// Key naming (hashtags for Redis Cluster compatibility):
//   tide:{queue}:available  — sorted set; score = priority * 1e12 + scheduled_at_ms
//   tide:{queue}:scheduled  — sorted set; score = scheduled_at_ms
//   tide:{queue}:stream     — Redis Stream (consumer group "workers")
//   tide:job:{id}           — Hash with all job fields
//   tide:unique:{hash}      — Key with TTL for unique-job enforcement

///|
pub struct RedisEngine {
  pool : @redis.Pool
  node_id : String
  mut shas : @redis.ScriptShas
}

///|
pub async fn RedisEngine::new(
  config : @redis.ClientConfig,
  pool_size : Int,
) -> RedisEngine raise EngineError {
  let pool = @redis.Pool::new(config, pool_size) catch {
    e => raise EngineError::RedisFailure("\{e}")
  }
  let engine : RedisEngine = {
    pool,
    node_id: generate_node_id(),
    shas: @redis.ScriptShas::create("", ""),
  }
  engine.ensure_scripts()
  engine
}

///|
pub async fn RedisEngine::ensure_scripts(
  self : RedisEngine,
) -> Unit raise EngineError {
  let (move_sha, unique_sha) = self.pool.with_conn(async fn(conn) {
    let m = conn.script_load(@redis.move_to_stream_script)
    let u = conn.script_load(@redis.unique_insert_script)
    (m, u)
  }) catch {
    e => raise EngineError::RedisFailure("\{e}")
  }
  self.shas = @redis.ScriptShas::create(move_sha, unique_sha)
}

///|
pub async fn RedisEngine::ensure_stream_group(
  self : RedisEngine,
  queue~ : String,
) -> Unit raise EngineError {
  self.pool.with_conn(async fn(conn) {
    conn.xgroup_create_mkstream(stream_key(queue), "workers")
  }) catch {
    e => raise EngineError::RedisFailure("\{e}")
  }
}

///|
pub async fn RedisEngine::insert_job(
  self : RedisEngine,
  job : @job.Job,
) -> @job.Job raise EngineError {
  let now = now_ms()
  let id = generate_job_id()
  let scheduled_at = if job.scheduled_at == 0L { now } else { job.scheduled_at }
  let job = { ..job, id, inserted_at: now, scheduled_at }
  let score = job.sorted_set_score()
  self.pool.with_conn(async fn(conn) {
    let _ = conn.hset(job_hash_key(id), job.to_hash_fields())
    let _ = conn.zadd(available_key(job.queue), score, id)
  }) catch {
    e => raise EngineError::RedisFailure("\{e}")
  }
  job
}

///|
pub async fn RedisEngine::insert_all(
  self : RedisEngine,
  jobs : Array[@job.Job],
) -> Array[@job.Job] raise EngineError {
  let inserted : Array[@job.Job] = []
  for job in jobs {
    inserted.push(self.insert_job(job))
  }
  inserted
}

///|
pub async fn RedisEngine::fetch_jobs(
  self : RedisEngine,
  queue~ : String,
  limit~ : Int,
) -> Array[@job.FetchedJob] raise EngineError {
  let avail_key = available_key(queue)
  let strm_key = stream_key(queue)
  let hash_prefix = "tide:job:"
  let move_sha = self.shas.move_to_stream
  let node_id = self.node_id
  self.pool.with_conn(async fn(conn) {
    // Atomically pop from available sorted set → stream.
    let _ = conn.evalsha(move_sha, [avail_key, strm_key, hash_prefix], [
      limit.to_string(),
    ])
    // Claim entries from the stream consumer group.
    let entries = conn.xreadgroup(
      "workers",
      node_id,
      strm_key,
      count=limit,
      block_ms=1000,
    )
    let now = now_ms()
    let fetched : Array[@job.FetchedJob] = []
    for entry in entries {
      let job_id = find_field(entry.fields, "job_id").unwrap_or("")
      if job_id.is_empty() {
        continue
      }
      let pairs = conn.hgetall(job_hash_key(job_id))
      match @job.Job::from_hash_fields(pairs) {
        Some(job) => {
          let new_attempted_by = job.attempted_by + [node_id]
          let _ = conn.hset(job_hash_key(job_id), [
            "attempted_at",
            now.to_string(),
            "attempted_by",
            @job.json_string_array(new_attempted_by),
          ])
          let updated = {
            ..job,
            attempted_at: Some(now),
            attempted_by: new_attempted_by,
          }
          fetched.push(@job.FetchedJob::new(updated, entry.stream_id))
        }
        None => () // corrupt hash; Lifeline will reclaim PEL entry
      }
    }
    fetched
  }) catch {
    e => raise EngineError::RedisFailure("\{e}")
  }
}

///|
pub async fn RedisEngine::complete_job(
  self : RedisEngine,
  fetched : @job.FetchedJob,
) -> Unit raise EngineError {
  let job = fetched.job
  let now = now_ms()
  self.pool.with_conn(async fn(conn) {
    let _ = conn.xack(stream_key(job.queue), "workers", fetched.stream_id)
    let _ = conn.hset(job_hash_key(job.id), [
      "state",
      @job.JobState::Completed.to_string(),
      "completed_at",
      now.to_string(),
    ])
  }) catch {
    e => raise EngineError::RedisFailure("\{e}")
  }
}

///|
pub async fn RedisEngine::error_job(
  self : RedisEngine,
  fetched : @job.FetchedJob,
  error_msg~ : String,
  next_scheduled_at~ : Int64?,
) -> Unit raise EngineError {
  let job = fetched.job
  let now = now_ms()
  self.pool.with_conn(async fn(conn) {
    let _ = conn.xack(stream_key(job.queue), "workers", fetched.stream_id)
    match next_scheduled_at {
      Some(scheduled_at) => {
        let new_attempt = job.attempt + 1
        let error_entry = "{\"message\":\"\{escape_for_json(error_msg)}\",\"at\":\{now}}"
        let new_errors = @job.json_string_array(job.errors + [error_entry])
        let _ = conn.hset(job_hash_key(job.id), [
          "state",
          @job.JobState::Retryable.to_string(),
          "attempt",
          new_attempt.to_string(),
          "scheduled_at",
          scheduled_at.to_string(),
          "errors",
          new_errors,
        ])
        let score = Int64::from_int(job.priority) * 1_000_000_000_000L +
          scheduled_at
        let _ = conn.zadd(available_key(job.queue), score, job.id)
      }
      None => {
        let _ = conn.hset(job_hash_key(job.id), [
          "state",
          @job.JobState::Discarded.to_string(),
          "discarded_at",
          now.to_string(),
        ])
      }
    }
  }) catch {
    e => raise EngineError::RedisFailure("\{e}")
  }
}

///|
pub async fn RedisEngine::snooze_job(
  self : RedisEngine,
  fetched : @job.FetchedJob,
  scheduled_at~ : Int64,
) -> Unit raise EngineError {
  let job = fetched.job
  self.pool.with_conn(async fn(conn) {
    let _ = conn.xack(stream_key(job.queue), "workers", fetched.stream_id)
    let _ = conn.hset(job_hash_key(job.id), [
      "state",
      @job.JobState::Scheduled.to_string(),
      "scheduled_at",
      scheduled_at.to_string(),
    ])
    let _ = conn.zadd(scheduled_key(job.queue), scheduled_at, job.id)
  }) catch {
    e => raise EngineError::RedisFailure("\{e}")
  }
}

///|
pub async fn RedisEngine::discard_job(
  self : RedisEngine,
  fetched : @job.FetchedJob,
) -> Unit raise EngineError {
  let job = fetched.job
  let now = now_ms()
  self.pool.with_conn(async fn(conn) {
    let _ = conn.xack(stream_key(job.queue), "workers", fetched.stream_id)
    let _ = conn.hset(job_hash_key(job.id), [
      "state",
      @job.JobState::Discarded.to_string(),
      "discarded_at",
      now.to_string(),
    ])
  }) catch {
    e => raise EngineError::RedisFailure("\{e}")
  }
}

///|
pub async fn RedisEngine::cancel_job(
  self : RedisEngine,
  id~ : String,
) -> Unit raise EngineError {
  let now = now_ms()
  // Look up the job first to get its queue (needed for sorted-set key).
  let maybe_job : @job.Job? = self.pool.with_conn(async fn(conn) {
    let pairs = conn.hgetall(job_hash_key(id))
    @job.Job::from_hash_fields(pairs)
  }) catch {
    e => raise EngineError::RedisFailure("\{e}")
  }
  let job = match maybe_job {
    None => raise EngineError::JobNotFound(id)
    Some(j) => j
  }
  self.pool.with_conn(async fn(conn) {
    let _ = conn.zrem(available_key(job.queue), id)
    let _ = conn.zrem(scheduled_key(job.queue), id)
    let _ = conn.hset(job_hash_key(id), [
      "state",
      @job.JobState::Cancelled.to_string(),
      "cancelled_at",
      now.to_string(),
    ])
  }) catch {
    e => raise EngineError::RedisFailure("\{e}")
  }
}

///|
pub async fn RedisEngine::retry_job(
  self : RedisEngine,
  id~ : String,
) -> Unit raise EngineError {
  let now = now_ms()
  // Look up the job first to get its queue and priority.
  let maybe_job : @job.Job? = self.pool.with_conn(async fn(conn) {
    let pairs = conn.hgetall(job_hash_key(id))
    @job.Job::from_hash_fields(pairs)
  }) catch {
    e => raise EngineError::RedisFailure("\{e}")
  }
  let job = match maybe_job {
    None => raise EngineError::JobNotFound(id)
    Some(j) => j
  }
  let score = Int64::from_int(job.priority) * 1_000_000_000_000L + now
  self.pool.with_conn(async fn(conn) {
    let _ = conn.hset(job_hash_key(id), [
      "state",
      @job.JobState::Available.to_string(),
      "scheduled_at",
      now.to_string(),
    ])
    let _ = conn.zadd(available_key(job.queue), score, id)
  }) catch {
    e => raise EngineError::RedisFailure("\{e}")
  }
}

///|
pub fn RedisEngine::prune_jobs(
  self : RedisEngine,
  queue~ : String,
  older_than_ms~ : Int64,
) -> Int {
  // TODO: implement prune scan — scan job hashes in terminal states
  // older than older_than_ms and delete them.
  let _ = (self, queue, older_than_ms)
  0
}

// ── String helpers ───────────────────────────────────────────────────────

fn escape_for_json(s : String) -> String {
  s
  .replace(old="\\", new="\\\\")
  .replace(old="\"", new="\\\"")
  .replace(old="\n", new="\\n")
  .replace(old="\r", new="\\r")
}

// ── Key helpers ──────────────────────────────────────────────────────────

///|
fn job_hash_key(id : String) -> String {
  "tide:job:\{id}"
}

///|
fn available_key(queue : String) -> String {
  "tide:{" + queue + "}:available"
}

///|
fn scheduled_key(queue : String) -> String {
  "tide:{" + queue + "}:scheduled"
}

///|
fn stream_key(queue : String) -> String {
  "tide:{" + queue + "}:stream"
}

///|
fn find_field(fields : Array[String], name : String) -> String? {
  let mut i = 0
  while i + 1 < fields.length() {
    if fields[i] == name {
      return Some(fields[i + 1])
    }
    i = i + 2
  }
  None
}

// ── Platform helpers ──────────────────────────────────────────────────────

let job_seq : Ref[Int64] = Ref(0L)

///|
fn now_ms() -> Int64 {
  @async.now()
}

///|
fn generate_job_id() -> String {
  job_seq.val = job_seq.val + 1L
  "j\{now_ms()}\{job_seq.val}"
}

///|
fn generate_node_id() -> String {
  "node-\{now_ms()}"
}
