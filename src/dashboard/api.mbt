// Dashboard query layer — Redis reads for queue stats and job browsing.
// All functions use the RedisEngine's connection pool directly.

// ── Types ─────────────────────────────────────────────────────────────────────

///|
pub struct QueueStats {
  queue : String
  available : Int
  scheduled : Int
  executing : Int
} derive(Debug)

// ── Queue stats ───────────────────────────────────────────────────────────────

///|
/// Return live counts (available, scheduled, executing) for a single queue.
pub async fn queue_stats(
  pool : @redis.Pool,
  queue : String,
) -> QueueStats raise Error {
  pool.with_conn(async fn(conn) {
    let avail_n = conn
      .command(["ZCARD", "tide:{" + queue + "}:available"])
      .as_int()
    let sched_n = conn
      .command(["ZCARD", "tide:{" + queue + "}:scheduled"])
      .as_int()
    // XPENDING stream group - + COUNT  →  one entry per in-flight job.
    let pending_resp = conn.command([
      "XPENDING",
      "tide:{" + queue + "}:stream",
      "workers",
      "-",
      "+",
      "500",
    ])
    let exec_n = match pending_resp {
      @redis.RespValue::Array(arr) => arr.length()
      _ => 0
    }
    {
      queue,
      available: avail_n.to_int(),
      scheduled: sched_n.to_int(),
      executing: exec_n,
    }
  })
}

// ── Job listing ───────────────────────────────────────────────────────────────

///|
/// List jobs for `queue` filtered by `state`.
///
/// For available/scheduled: read from the sorted set (fast, O(log N + M)).
/// For executing: read from XPENDING (fast).
/// For terminal states (completed/discarded/cancelled/retryable): SCAN all
///   `tide:job:*` hashes and filter — O(N) on total jobs, capped at 1 000 scanned.
pub async fn list_jobs(
  pool : @redis.Pool,
  queue : String,
  state : String,
  limit : Int,
  offset : Int,
) -> Array[@job.Job] raise Error {
  pool.with_conn(async fn(conn) {
    match state {
      "available" =>
        list_from_zset(conn, "tide:{" + queue + "}:available", offset, limit)
      "scheduled" =>
        list_from_zset(conn, "tide:{" + queue + "}:scheduled", offset, limit)
      "executing" => list_executing(conn, queue, offset, limit)
      _ => list_terminal(conn, queue, state, offset, limit)
    }
  })
}

///|
async fn list_from_zset(
  conn : @redis.Connection,
  key : String,
  offset : Int,
  limit : Int,
) -> Array[@job.Job] raise @redis.RedisError {
  let end_idx = offset + limit - 1
  let ids = conn
    .command(["ZRANGE", key, offset.to_string(), end_idx.to_string()])
    .as_array()
    .map(fn(v) { v.as_string() catch { _ => "" } })
    .filter(fn(s) { !s.is_empty() })
  let jobs : Array[@job.Job] = []
  for id in ids {
    let pairs = conn.hgetall("tide:job:\{id}")
    match @job.Job::from_hash_fields(pairs) {
      Some(j) => jobs.push(j)
      None => ()
    }
  }
  jobs
}

///|
async fn list_executing(
  conn : @redis.Connection,
  queue : String,
  offset : Int,
  limit : Int,
) -> Array[@job.Job] raise @redis.RedisError {
  let pending = conn
    .command([
      "XPENDING",
      "tide:{" + queue + "}:stream",
      "workers",
      "-",
      "+",
      (offset + limit).to_string(),
    ])
    .as_array()
  let jobs : Array[@job.Job] = []
  let mut i = offset
  while i < pending.length() && jobs.length() < limit {
    // Each entry: [stream-id, consumer-name, idle-ms, delivery-count]
    let entry = pending[i].as_array()
    if entry.length() > 0 {
      // The stream entry id looks like "0-1"; the job hash key stores
      // the job id in the "job_id" field of the stream entry.
      // We retrieve the job id via XRANGE and then HGETALL.
      let stream_id = entry[0].as_string() catch { _ => "" }
      if !stream_id.is_empty() {
        let range = conn
          .command([
            "XRANGE",
            "tide:{" + queue + "}:stream",
            stream_id,
            stream_id,
          ])
          .as_array()
        if range.length() > 0 {
          let msg = range[0].as_array()
          if msg.length() >= 2 {
            let fields = msg[1].as_array()
            let job_id = find_field_value(fields, "job_id")
            if !job_id.is_empty() {
              let pairs = conn.hgetall("tide:job:\{job_id}")
              match @job.Job::from_hash_fields(pairs) {
                Some(j) => jobs.push(j)
                None => ()
              }
            }
          }
        }
      }
    }
    i = i + 1
  }
  jobs
}

///|
async fn list_terminal(
  conn : @redis.Connection,
  queue : String,
  state : String,
  offset : Int,
  limit : Int,
) -> Array[@job.Job] raise @redis.RedisError {
  // SCAN all tide:job:* keys and filter by queue + state.
  // Capped at 1 000 keys to limit latency.
  let matching : Array[@job.Job] = []
  let mut cursor = "0"
  let mut scanned = 0
  while scanned < 1000 && matching.length() < offset + limit {
    let (next, keys) = conn.scan(cursor, match_pattern="tide:job:*", count=100)
    cursor = next
    for key in keys {
      let pairs = conn.hgetall(key)
      match @job.Job::from_hash_fields(pairs) {
        Some(j) if j.queue == queue && j.state.to_string() == state =>
          matching.push(j)
        _ => ()
      }
      scanned = scanned + 1
    }
    if cursor == "0" {
      break
    }
  }
  // Return the requested page.
  let result : Array[@job.Job] = []
  let mut i = offset
  while i < matching.length() && result.length() < limit {
    result.push(matching[i])
    i = i + 1
  }
  result
}

// ── Single job ────────────────────────────────────────────────────────────────

///|
pub async fn get_job(pool : @redis.Pool, id : String) -> @job.Job? raise Error {
  pool.with_conn(async fn(conn) {
    let pairs = conn.hgetall("tide:job:\{id}")
    if pairs.is_empty() {
      None
    } else {
      @job.Job::from_hash_fields(pairs)
    }
  })
}

// ── Management ────────────────────────────────────────────────────────────────

///|
pub async fn retry_job(
  engine : @engine.RedisEngine,
  id : String,
) -> Unit raise Error {
  engine.retry_job(id~)
}

///|
pub async fn cancel_job(
  engine : @engine.RedisEngine,
  id : String,
) -> Unit raise Error {
  engine.cancel_job(id~)
}

///|
pub async fn delete_job(pool : @redis.Pool, id : String) -> Unit raise Error {
  pool.with_conn(async fn(conn) {
    let _ = conn.command(["DEL", "tide:job:\{id}"])
  })
}

// ── Internal helpers ──────────────────────────────────────────────────────────

///|
fn find_field_value(fields : Array[@redis.RespValue], name : String) -> String {
  let mut i = 0
  while i + 1 < fields.length() {
    let k = fields[i].as_string() catch { _ => "" }
    if k == name {
      return fields[i + 1].as_string() catch { _ => "" }
    }
    i = i + 2
  }
  ""
}
