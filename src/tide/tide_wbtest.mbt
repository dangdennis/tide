// End-to-end integration test for the full tide pipeline.
// Requires: docker-compose up -d redis  (Redis on 127.0.0.1:6379)
// Run:      moon test --target native

// ── Helpers ───────────────────────────────────────────────────────────────────

///|
fn get_redis_field(pairs : Array[String], name : String) -> String? {
  let mut i = 0
  while i + 1 < pairs.length() {
    if pairs[i] == name {
      return Some(pairs[i + 1])
    }
    i += 2
  }
  None
}

///|
fn e2e_config(queue : String) -> Config {
  {
    ..Config::default(),
    queues: Map([(queue, 1)]),
    pool_size: 2,
    name: "tide-e2e",
    shutdown_grace_ms: 3000,
  }
}

///|
async fn e2e_cleanup(
  instance : Instance,
  queue : String,
  ids : Array[String],
) -> Unit {
  instance.engine.pool.with_conn(async fn(conn) {
    for id in ids {
      let _ = conn.del("tide:job:\{id}")
    }
    let _ = conn.del("tide:{\{queue}}:available")
    let _ = conn.del("tide:{\{queue}}:scheduled")
    let _ = conn.del("tide:{\{queue}}:stream")
  }) catch {
    _ => ()
  }
  instance.engine.ensure_stream_group(queue~) catch {
    _ => ()
  }
}

// ── End-to-end test ───────────────────────────────────────────────────────────

///|
async test "itest: end-to-end insert → run → perform → complete" {
  let q = "it-e2e"
  let instance = Instance::start(e2e_config(q))
  e2e_cleanup(instance, q, [])

  // Signal from inside the worker closure back to the test.
  let done : Ref[Bool] = Ref(false)
  let cond = @cond_var.Cond()

  instance.register("echo", fn(_job) {
    done.val = true
    cond.signal()
    @worker.PerformResult::Ok
  })

  let inserted = instance.insert(@job.Job::new(q, "echo"))

  @async.with_task_group(async fn(tg) {
    // Run the instance; the group waits for it (no_wait defaults to false).
    tg.spawn_bg(allow_failure=true) <| () => {
      instance.run() catch {
        _ => ()
      }
    }

    // Block until the worker signals or 5 s elapses.
    let _ = @async.with_timeout_opt(5000, async fn() { cond.wait() })

    instance.stop()

    assert_true(done.val)

    let pairs = instance.engine.pool.with_conn(async fn(conn) {
      conn.hgetall("tide:job:\{inserted.id}")
    }) catch {
      _ => []
    }
    assert_eq(get_redis_field(pairs, "state").unwrap_or(""), "completed")
  })

  e2e_cleanup(instance, q, [inserted.id])
}

///|
async test "itest: unknown worker is discarded" {
  let q = "it-e2e-discard"
  let instance = Instance::start(e2e_config(q))
  e2e_cleanup(instance, q, [])

  let done : Ref[Bool] = Ref(false)
  let cond = @cond_var.Cond()

  // No worker registered for "ghost" — executor should discard it.
  // We detect completion by polling: the job hash state changes to "discarded".
  // Use a separate known worker to drive the signal.
  instance.register("probe", fn(_job) {
    done.val = true
    cond.signal()
    @worker.PerformResult::Ok
  })

  let ghost = instance.insert(@job.Job::new(q, "ghost"))
  let probe = instance.insert(@job.Job::new(q, "probe"))

  @async.with_task_group(async fn(tg) {
    tg.spawn_bg(allow_failure=true) <| () => {
      instance.run() catch {
        _ => ()
      }
    }

    let _ = @async.with_timeout_opt(5000, async fn() { cond.wait() })

    instance.stop()

    assert_true(done.val)

    let ghost_pairs = instance.engine.pool.with_conn(async fn(conn) {
      conn.hgetall("tide:job:\{ghost.id}")
    }) catch {
      _ => []
    }
    assert_eq(get_redis_field(ghost_pairs, "state").unwrap_or(""), "discarded")

    let probe_pairs = instance.engine.pool.with_conn(async fn(conn) {
      conn.hgetall("tide:job:\{probe.id}")
    }) catch {
      _ => []
    }
    assert_eq(get_redis_field(probe_pairs, "state").unwrap_or(""), "completed")
  })

  e2e_cleanup(instance, q, [ghost.id, probe.id])
}

// ── Executor error paths ──────────────────────────────────────────────────────

///|
async test "itest: failing worker with attempts remaining transitions to retryable" {
  let q = "it-e2e-retry"
  let instance = Instance::start(e2e_config(q))
  e2e_cleanup(instance, q, [])

  let called : Ref[Bool] = Ref(false)
  let cond = @cond_var.Cond()

  instance.register("failing", _job => {
    called.val = true
    cond.signal()
    raise Failure::Failure("simulated failure")
  })

  // max_attempts=3 so the first failure is retryable, not discarded.
  let inserted = instance.insert(
    @job.Job::new(q, "failing").with_max_attempts(3),
  )

  @async.with_task_group(async fn(tg) {
    tg.spawn_bg(allow_failure=true) <| () => {
      instance.run() catch {
        _ => ()
      }
    }
    let _ = @async.with_timeout_opt(5000, async fn() { cond.wait() })
    instance.stop()
    assert_true(called.val)
    let pairs = instance.engine.pool.with_conn(async fn(conn) {
      conn.hgetall("tide:job:\{inserted.id}")
    }) catch {
      _ => []
    }
    assert_eq(get_redis_field(pairs, "state").unwrap_or(""), "retryable")
    assert_eq(get_redis_field(pairs, "attempt").unwrap_or(""), "1")
  })

  e2e_cleanup(instance, q, [inserted.id])
}

///|
async test "itest: failing worker with max_attempts=1 is discarded after one failure" {
  let q = "it-e2e-discard-err"
  let instance = Instance::start(e2e_config(q))
  e2e_cleanup(instance, q, [])

  let called : Ref[Bool] = Ref(false)
  let cond = @cond_var.Cond()

  instance.register("always-fail", _job => {
    called.val = true
    cond.signal()
    raise Failure::Failure("fatal error")
  })

  let inserted = instance.insert(
    @job.Job::new(q, "always-fail").with_max_attempts(1),
  )

  @async.with_task_group(async fn(tg) {
    tg.spawn_bg(allow_failure=true) <| () => {
      instance.run() catch {
        _ => ()
      }
    }
    let _ = @async.with_timeout_opt(5000, async fn() { cond.wait() })
    instance.stop()
    assert_true(called.val)
    let pairs = instance.engine.pool.with_conn(async fn(conn) {
      conn.hgetall("tide:job:\{inserted.id}")
    }) catch {
      _ => []
    }
    assert_eq(get_redis_field(pairs, "state").unwrap_or(""), "discarded")
  })

  e2e_cleanup(instance, q, [inserted.id])
}

///|
async test "itest: worker returning Discard transitions job to discarded regardless of attempts" {
  let q = "it-e2e-discard-result"
  let instance = Instance::start(e2e_config(q))
  e2e_cleanup(instance, q, [])

  let called : Ref[Bool] = Ref(false)
  let cond = @cond_var.Cond()

  instance.register("self-discard", _job => {
    called.val = true
    cond.signal()
    @worker.PerformResult::Discard("intentional")
  })

  // max_attempts=5 to confirm Discard is not attempt-count dependent.
  let inserted = instance.insert(
    @job.Job::new(q, "self-discard").with_max_attempts(5),
  )

  @async.with_task_group(async fn(tg) {
    tg.spawn_bg(allow_failure=true) <| () => {
      instance.run() catch {
        _ => ()
      }
    }
    let _ = @async.with_timeout_opt(5000, async fn() { cond.wait() })
    instance.stop()
    assert_true(called.val)
    let pairs = instance.engine.pool.with_conn(async fn(conn) {
      conn.hgetall("tide:job:\{inserted.id}")
    }) catch {
      _ => []
    }
    assert_eq(get_redis_field(pairs, "state").unwrap_or(""), "discarded")
  })

  e2e_cleanup(instance, q, [inserted.id])
}

///|
async test "itest: worker returning Snooze transitions job to scheduled" {
  let q = "it-e2e-snooze"
  let instance = Instance::start(e2e_config(q))
  e2e_cleanup(instance, q, [])

  let done : Ref[Bool] = Ref(false)
  let cond = @cond_var.Cond()

  instance.register("lazy", _job => {
    done.val = true
    cond.signal()
    @worker.PerformResult::Snooze(60)
  })

  let inserted = instance.insert(@job.Job::new(q, "lazy"))

  @async.with_task_group(async fn(tg) {
    tg.spawn_bg(allow_failure=true) <| () => {
      instance.run() catch {
        _ => ()
      }
    }

    let _ = @async.with_timeout_opt(5000, async fn() { cond.wait() })

    instance.stop()

    assert_true(done.val)

    let pairs = instance.engine.pool.with_conn(async fn(conn) {
      conn.hgetall("tide:job:\{inserted.id}")
    }) catch {
      _ => []
    }
    assert_eq(get_redis_field(pairs, "state").unwrap_or(""), "scheduled")
  })

  e2e_cleanup(instance, q, [inserted.id])
}
