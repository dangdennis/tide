// Integration tests for RedisEngine.
// Requires: docker-compose up -d redis  (Redis on 127.0.0.1:6379)
// Run:      moon test --target native

let test_redis_url = "redis://127.0.0.1:6379"

// ── Helpers ───────────────────────────────────────────────────────────────────

///|
async test "itest: minimal connection smoke test" {
  let config = @redis.ClientConfig::from_url(test_redis_url) catch {
    e => raise e
  }
  let conn = @redis.Connection::open(config) catch { e => raise e }
  let reply = conn.command(["PING"]) catch { e => raise e }
  inspect(reply, content="SimpleStr(\"PONG\")")
  conn.close()
}

async fn itest_engine() -> RedisEngine raise EngineError {
  let config = @redis.ClientConfig::from_url(test_redis_url) catch {
    e => raise EngineError::RedisFailure("\{e}")
  }
  let eng = RedisEngine::new(config, 2)
  eng
}

// Delete all Redis keys owned by a test queue plus specific job hashes.
async fn cleanup(
  eng : RedisEngine,
  queue : String,
  ids : Array[String],
) -> Unit {
  eng.pool.with_conn(async fn(conn) {
    for id in ids {
      let _ = conn.del(job_hash_key(id))
    }
    let _ = conn.del(available_key(queue))
    let _ = conn.del(scheduled_key(queue))
    let _ = conn.del(stream_key(queue))
  }) catch {
    _ => ()
  }
}

// ── insert_job ────────────────────────────────────────────────────────────────

///|
async test "itest: insert_job populates id and non-zero timestamps" {
  println("t1: start")
  let q = "it-ts"
  println("t1: calling itest_engine")
  let eng = itest_engine()
  println("t1: engine ready, calling cleanup")
  cleanup(eng, q, [])
  println("t1: cleanup done, ensure_stream_group")
  eng.ensure_stream_group(queue=q)
  println("t1: ensure done, insert_job")
  let inserted = eng.insert_job(@job.Job::new(q, "W"))
  println("t1: insert done, asserting")
  assert_false(inserted.id.is_empty())
  assert_true(inserted.inserted_at > 0L)
  assert_true(inserted.scheduled_at > 0L)
  println("t1: cleanup final")
  cleanup(eng, q, [inserted.id])
  println("t1: done")
}

///|
async test "itest: insert_job stores hash with correct state and worker" {
  let q = "it-hash"
  let eng = itest_engine()
  cleanup(eng, q, [])
  eng.ensure_stream_group(queue=q)
  let inserted = eng.insert_job(@job.Job::new(q, "HashWorker"))
  let pairs = eng.pool.with_conn(async fn(conn) {
    conn.hgetall(job_hash_key(inserted.id))
  })
  assert_false(pairs.is_empty())
  assert_eq(find_field(pairs, "state").unwrap_or(""), "available")
  assert_eq(find_field(pairs, "worker").unwrap_or(""), "HashWorker")
  assert_eq(find_field(pairs, "queue").unwrap_or(""), q)
  cleanup(eng, q, [inserted.id])
}

///|
async test "itest: insert_job multiple inserts produce unique IDs" {
  let q = "it-uniq"
  let eng = itest_engine()
  cleanup(eng, q, [])
  eng.ensure_stream_group(queue=q)
  let ids = []
  for _ in 0..<5 {
    let inserted = eng.insert_job(@job.Job::new(q, "W"))
    ids.push(inserted.id)
  }
  // All IDs must be distinct.
  for i in 0..<ids.length() {
    let mut j = i + 1
    while j < ids.length() {
      assert_false(ids[i] == ids[j])
      j += 1
    }
  }
  cleanup(eng, q, ids)
}

// ── fetch_jobs (move_to_stream Lua script) ────────────────────────────────────

///|
async test "itest: fetch_jobs returns inserted job via Lua script" {
  let q = "it-fetch"
  let eng = itest_engine()
  cleanup(eng, q, [])
  eng.ensure_stream_group(queue=q)
  let inserted = eng.insert_job(@job.Job::new(q, "FetchWorker"))
  let fetched = eng.fetch_jobs(queue=q, limit=1)
  assert_eq(fetched.length(), 1)
  assert_eq(fetched[0].job.id, inserted.id)
  assert_eq(fetched[0].job.worker, "FetchWorker")
  assert_false(fetched[0].stream_id.is_empty())
  cleanup(eng, q, [inserted.id])
}

///|
async test "itest: fetch_jobs sets job state to executing in Redis" {
  let q = "it-exec-state"
  let eng = itest_engine()
  cleanup(eng, q, [])
  eng.ensure_stream_group(queue=q)
  let inserted = eng.insert_job(@job.Job::new(q, "W"))
  let _ = eng.fetch_jobs(queue=q, limit=1)
  let pairs = eng.pool.with_conn(async fn(conn) {
    conn.hgetall(job_hash_key(inserted.id))
  })
  assert_eq(find_field(pairs, "state").unwrap_or(""), "executing")
  cleanup(eng, q, [inserted.id])
}

// ── complete_job ──────────────────────────────────────────────────────────────

///|
async test "itest: complete_job transitions state to completed" {
  let q = "it-complete"
  let eng = itest_engine()
  cleanup(eng, q, [])
  eng.ensure_stream_group(queue=q)
  let inserted = eng.insert_job(@job.Job::new(q, "W"))
  let fetched = eng.fetch_jobs(queue=q, limit=1)
  assert_eq(fetched.length(), 1)
  eng.complete_job(fetched[0])
  let pairs = eng.pool.with_conn(async fn(conn) {
    conn.hgetall(job_hash_key(inserted.id))
  })
  assert_eq(find_field(pairs, "state").unwrap_or(""), "completed")
  assert_false(find_field(pairs, "completed_at").unwrap_or("").is_empty())
  cleanup(eng, q, [inserted.id])
}

// ── error_job ─────────────────────────────────────────────────────────────────

///|
async test "itest: error_job with retry increments attempt and reschedules" {
  let q = "it-retry"
  let eng = itest_engine()
  cleanup(eng, q, [])
  eng.ensure_stream_group(queue=q)
  let job = @job.Job::new(q, "W").with_max_attempts(3)
  let inserted = eng.insert_job(job)
  let fetched = eng.fetch_jobs(queue=q, limit=1)
  assert_eq(fetched.length(), 1)
  let next_at = @async.now() + 5000L
  eng.error_job(
    fetched[0],
    error_msg="simulated error",
    next_scheduled_at=Some(next_at),
  )
  let pairs = eng.pool.with_conn(async fn(conn) {
    conn.hgetall(job_hash_key(inserted.id))
  })
  assert_eq(find_field(pairs, "state").unwrap_or(""), "retryable")
  assert_eq(find_field(pairs, "attempt").unwrap_or(""), "1")
  cleanup(eng, q, [inserted.id])
}

///|
async test "itest: error_job exhausted transitions to discarded" {
  let q = "it-discard"
  let eng = itest_engine()
  cleanup(eng, q, [])
  eng.ensure_stream_group(queue=q)
  let job = @job.Job::new(q, "W").with_max_attempts(1)
  let inserted = eng.insert_job(job)
  let fetched = eng.fetch_jobs(queue=q, limit=1)
  assert_eq(fetched.length(), 1)
  eng.error_job(fetched[0], error_msg="fatal", next_scheduled_at=None)
  let pairs = eng.pool.with_conn(async fn(conn) {
    conn.hgetall(job_hash_key(inserted.id))
  })
  assert_eq(find_field(pairs, "state").unwrap_or(""), "discarded")
  cleanup(eng, q, [inserted.id])
}

// ── snooze_job ────────────────────────────────────────────────────────────────

///|
async test "itest: snooze_job transitions state to scheduled" {
  let q = "it-snooze"
  let eng = itest_engine()
  cleanup(eng, q, [])
  eng.ensure_stream_group(queue=q)
  let inserted = eng.insert_job(@job.Job::new(q, "W"))
  let fetched = eng.fetch_jobs(queue=q, limit=1)
  assert_eq(fetched.length(), 1)
  let wake_at = @async.now() + 60000L
  eng.snooze_job(fetched[0], scheduled_at=wake_at)
  let pairs = eng.pool.with_conn(async fn(conn) {
    conn.hgetall(job_hash_key(inserted.id))
  })
  assert_eq(find_field(pairs, "state").unwrap_or(""), "scheduled")
  cleanup(eng, q, [inserted.id])
}

// ── cancel_job ────────────────────────────────────────────────────────────────

///|
async test "itest: cancel_job transitions state to cancelled" {
  let q = "it-cancel"
  let eng = itest_engine()
  cleanup(eng, q, [])
  eng.ensure_stream_group(queue=q)
  let inserted = eng.insert_job(@job.Job::new(q, "W"))
  eng.cancel_job(id=inserted.id)
  let pairs = eng.pool.with_conn(async fn(conn) {
    conn.hgetall(job_hash_key(inserted.id))
  })
  assert_eq(find_field(pairs, "state").unwrap_or(""), "cancelled")
  cleanup(eng, q, [inserted.id])
}

// ── discard_job ───────────────────────────────────────────────────────────────

///|
async test "itest: discard_job transitions state to discarded" {
  let q = "it-discard2"
  let eng = itest_engine()
  cleanup(eng, q, [])
  eng.ensure_stream_group(queue=q)
  let inserted = eng.insert_job(@job.Job::new(q, "W"))
  let fetched = eng.fetch_jobs(queue=q, limit=1)
  assert_eq(fetched.length(), 1)
  eng.discard_job(fetched[0])
  let pairs = eng.pool.with_conn(async fn(conn) {
    conn.hgetall(job_hash_key(inserted.id))
  })
  assert_eq(find_field(pairs, "state").unwrap_or(""), "discarded")
  cleanup(eng, q, [inserted.id])
}

// ── prune_jobs ────────────────────────────────────────────────────────────────

///|
async test "itest: prune_jobs deletes completed jobs older than threshold" {
  let q = "it-prune"
  let eng = itest_engine()
  cleanup(eng, q, [])
  eng.ensure_stream_group(queue=q)
  // Insert and immediately complete two jobs.
  let j1 = eng.insert_job(@job.Job::new(q, "W"))
  let j2 = eng.insert_job(@job.Job::new(q, "W"))
  let fetched = eng.fetch_jobs(queue=q, limit=2)
  assert_eq(fetched.length(), 2)
  for f in fetched {
    eng.complete_job(f)
  }
  // With a 0 ms threshold everything completed "before now" is eligible.
  let pruned = eng.prune_jobs(queue=q, older_than_ms=0L)
  assert_true(pruned >= 2)
  // Verify the hashes were deleted.
  let ids = [j1.id, j2.id]
  eng.pool.with_conn(async fn(conn) {
    for id in ids {
      let pairs = conn.hgetall("tide:job:\{id}")
      assert_true(pairs.is_empty())
    }
  }) catch {
    _ => ()
  }
  cleanup(eng, q, [])
}

///|
async test "itest: prune_jobs does not delete jobs newer than threshold" {
  let q = "it-prune-keep"
  let eng = itest_engine()
  cleanup(eng, q, [])
  eng.ensure_stream_group(queue=q)
  let inserted = eng.insert_job(@job.Job::new(q, "W"))
  let fetched = eng.fetch_jobs(queue=q, limit=1)
  assert_eq(fetched.length(), 1)
  eng.complete_job(fetched[0])
  // A very large threshold means the job is "too new" to prune.
  let pruned = eng.prune_jobs(queue=q, older_than_ms=9999999999000L)
  assert_eq(pruned, 0)
  cleanup(eng, q, [inserted.id])
}

// ── round-trip: tags and errors survive Redis serialization ──────────────────

///|
async test "itest: job with tags round-trips through Redis" {
  let q = "it-tags"
  let eng = itest_engine()
  cleanup(eng, q, [])
  eng.ensure_stream_group(queue=q)
  let job = @job.Job::new(q, "W").with_tags(["billing", "urgent"])
  let inserted = eng.insert_job(job)
  let fetched = eng.fetch_jobs(queue=q, limit=1)
  assert_eq(fetched.length(), 1)
  let ftags = fetched[0].job.tags
  assert_eq(ftags.length(), 2)
  assert_eq(ftags[0], "billing")
  assert_eq(ftags[1], "urgent")
  cleanup(eng, q, [inserted.id])
}
