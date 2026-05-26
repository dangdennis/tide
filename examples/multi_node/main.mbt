// multi_node — proves that multi-node coordination works across two tide
// instances sharing the same Redis backend.
//
// Both instances are started in-process to keep the example self-contained;
// each generates a unique node_id at startup, so they are genuinely separate
// consumers in the Redis Stream consumer group and separate peers in the
// election protocol.
//
// Three proofs, executed sequentially:
//
//   PROOF 1 — Leader election
//     Both nodes compete for the same Redis key (tide:peer:demo-cluster).
//     After the first election tick, exactly one node holds the key.
//     assert: alpha.is_leader() XOR beta.is_leader()
//
//   PROOF 2 — Load distribution
//     20 "Task" jobs are inserted into the shared queue.  Each node's fetch
//     loop races to claim jobs via XREADGROUP.  Per-node worker closures
//     count completions independently.
//     assert: alpha_done > 0 AND beta_done > 0
//
//   PROOF 3 — Cancel-signal propagation
//     8 "SlowTask" jobs tagged [to-cancel] are inserted and immediately
//     cancelled via alpha.cancel_by_tag.  cancel_by_tag publishes a pub/sub
//     signal for each cancelled ID so that beta's pre-dispatch notifier check
//     also skips any it may have already claimed into its PEL.
//     assert: cancel_executed == 0  (no SlowTask worker ever ran)
//
// Prerequisites:  docker-compose up -d redis
// Run:            moon run examples/multi_node --target native

// ── Helpers ───────────────────────────────────────────────────────────────────

///|
fn pass(label : String) -> Unit {
  println("  ✓ PASS  \{label}")
}

///|
fn fail(label : String) -> Unit {
  println("  ✗ FAIL  \{label}")
}

// ── Main ──────────────────────────────────────────────────────────────────────

///|
async fn main {
  println("=== tide multi-node proof ===")
  println("Connecting to Redis at 127.0.0.1:6379 …\n")

  // ── Build configs ────────────────────────────────────────────────────────
  //
  // Both nodes share the same `name`, which determines:
  //   • election key:    tide:peer:demo-cluster
  //   • notify channel:  tide:notify:demo-cluster
  //
  // Each Instance gets its own `workers` Map so that alpha.register() and
  // beta.register() write to separate maps and can hold different closures
  // for the same worker name.
  //
  // concurrency=2 per node → up to 4 concurrent jobs total; enough to ensure
  // both nodes stay busy even if one is slightly slower to claim.

  let alpha_config : @tide.Config = {
    ..@tide.Config::default(),
    queues: Map([("tasks", 2)]),
    pool_size: 5,
    name: "demo-cluster",
    peer_mode: @tide.PeerMode::Redis,
    notify_cancel: true,
    shutdown_grace_ms: 2000,
    lifeline_rescue_after_ms: 5000L,
    workers: Map([]),
  }
  let beta_config : @tide.Config = { ..alpha_config, workers: Map([]) } // separate map — prevents alpha.register from affecting beta

  let alpha = @tide.Instance::start(alpha_config) catch {
    e => {
      println("ERROR starting alpha: \{e}")
      panic()
    }
  }
  let beta = @tide.Instance::start(beta_config) catch {
    e => {
      println("ERROR starting beta: \{e}")
      panic()
    }
  }

  println("alpha node_id: \{alpha.engine.node_id}")
  println("beta  node_id: \{beta.engine.node_id}\n")

  // ── Shared counters ───────────────────────────────────────────────────────

  let alpha_done : Ref[Int] = Ref(0)
  let beta_done : Ref[Int] = Ref(0)
  let cancel_executed : Ref[Int] = Ref(0) // must stay 0 through Proof 3
  let phase_total : Ref[Int] = Ref(0)
  let phase_finished : Ref[Int] = Ref(0)
  let phase_done = @cond_var.Cond()

  // ── Register workers ─────────────────────────────────────────────────────
  //
  // "Task" has different closures on alpha and beta; whichever node's event
  // loop fetches the job calls its own closure, proving distribution.
  //
  // "SlowTask" is identical on both nodes and increments cancel_executed.
  // If cancellation works correctly, this counter stays at 0.

  alpha.register("Task", fn(job) {
    alpha_done.val += 1
    phase_finished.val += 1
    println("  [alpha] Task \{job.id}")
    if phase_finished.val >= phase_total.val {
      phase_done.signal()
    }
    @worker.PerformResult::Ok
  })

  beta.register("Task", fn(job) {
    beta_done.val += 1
    phase_finished.val += 1
    println("  [beta]  Task \{job.id}")
    if phase_finished.val >= phase_total.val {
      phase_done.signal()
    }
    @worker.PerformResult::Ok
  })

  alpha.register("SlowTask", fn(job) {
    cancel_executed.val += 1
    println(
      "  [alpha] SlowTask \{job.id}  ← UNEXPECTED: should have been cancelled",
    )
    @worker.PerformResult::Ok
  })

  beta.register("SlowTask", fn(job) {
    cancel_executed.val += 1
    println(
      "  [beta]  SlowTask \{job.id}  ← UNEXPECTED: should have been cancelled",
    )
    @worker.PerformResult::Ok
  })

  // ── Run ───────────────────────────────────────────────────────────────────

  @async.with_task_group(async fn(tg) {
    tg.spawn_bg(allow_failure=true) <| () => { alpha.run() catch { _ => () } }
    tg.spawn_bg(allow_failure=true) <| () => { beta.run() catch { _ => () } }

    // ════════════════════════════════════════════════════════════════════════
    // PROOF 1: Leader election
    // ════════════════════════════════════════════════════════════════════════
    println(
      "── PROOF 1: Leader election ─────────────────────────────────────",
    )
    //
    // RedisPeer::run() fires tick() immediately on its first iteration (no
    // leading sleep), so both nodes attempt election as soon as their peer
    // loops start.  We sleep 500 ms — enough for two Redis round-trips — then
    // read the in-memory leader flag.
    println("  waiting 500 ms for election ticks …")
    @async.sleep(500)

    let a_leads = alpha.is_leader()
    let b_leads = beta.is_leader()
    println("  alpha.is_leader() = \{a_leads}")
    println("  beta.is_leader()  = \{b_leads}")

    if a_leads != b_leads {
      let leader = if a_leads { "alpha" } else { "beta" }
      let follower = if a_leads { "beta" } else { "alpha" }
      pass(
        "\{leader} is leader, \{follower} is follower — exactly one leader",
      )
    } else {
      fail(
        "both nodes report is_leader=\{a_leads}; expected exactly one leader",
      )
    }

    // ════════════════════════════════════════════════════════════════════════
    // PROOF 2: Load distribution
    // ════════════════════════════════════════════════════════════════════════
    println(
      "\n── PROOF 2: Load distribution ───────────────────────────────────",
    )
    //
    // Insert 20 "Task" jobs.  Both nodes' fetch loops race via XREADGROUP; the
    // per-node worker closures count completions.  The proof holds if BOTH
    // alpha_done and beta_done are non-zero.

    let n = 20
    phase_total.val = n
    phase_finished.val = 0
    alpha_done.val = 0
    beta_done.val = 0
    println("  inserting \{n} Task jobs …")

    for _ in 0..<n {
      let _ = alpha.insert(@job.Job::new("tasks", "Task")) catch {
        e => {
          println("  insert error: \{e}")
          panic()
        }
      }
    }

    let _ = @async.with_timeout_opt(15000, async fn() { phase_done.wait() })
    println("  alpha processed: \{alpha_done.val}")
    println("  beta  processed: \{beta_done.val}")
    println("  total:           \{alpha_done.val + beta_done.val}/\{n}")

    if alpha_done.val > 0 && beta_done.val > 0 {
      pass(
        "both nodes processed jobs (alpha=\{alpha_done.val}, beta=\{beta_done.val})",
      )
    } else {
      fail(
        "one node processed nothing — alpha=\{alpha_done.val}, beta=\{beta_done.val}",
      )
    }

    // ════════════════════════════════════════════════════════════════════════
    // PROOF 3: Cancel-signal propagation
    // ════════════════════════════════════════════════════════════════════════
    println(
      "\n── PROOF 3: Cancel-signal propagation ───────────────────────────",
    )
    //
    // Mechanism:
    //   cancel_by_tag (on alpha):
    //     1. SCAN all tide:job:* hashes, marks Available/Scheduled/Retryable
    //        jobs as Cancelled and removes them from the sorted set.
    //     2. Publishes a pub/sub cancel signal on tide:notify:demo-cluster
    //        for every cancelled job ID.
    //   beta's notifier listener receives those signals and marks them in its
    //   local in-memory map.  The fetch loop's pre-dispatch check then skips
    //   any fetched job whose ID is in that map.
    //
    // Timing:
    //   After the Proof-2 burst both fetch loops enter XREADGROUP(block_ms=1000).
    //   We sleep 600 ms so they are deep into that wait.  The insert+cancel
    //   sequence runs entirely within the remaining ~400 ms window before the
    //   next move_to_stream Lua call — so the jobs are removed from the sorted
    //   set before either node can move them into the stream.

    println(
      "  sleeping 600 ms so fetch loops are deep in their XREADGROUP wait …",
    )
    @async.sleep(600)

    let nc = 8
    println("  inserting \{nc} SlowTask jobs tagged [to-cancel] …")
    for i in 0..<nc {
      let job = @job.Job::new("tasks", "SlowTask")
        .with_args("{\"i\":\{i}}")
        .with_tags(["to-cancel"])
      let ins = alpha.insert(job) catch {
        e => {
          println("  insert error: \{e}")
          panic()
        }
      }
      println("  [main] enqueued \{ins.id}")
    }

    // cancel_by_tag atomically cancels all Available tagged jobs in Redis and
    // publishes one pub/sub signal per job so that beta's notifier also learns
    // about the cancellations immediately.
    println("  calling alpha.cancel_by_tag(\"tasks\", \"to-cancel\") …")
    let n_cancelled = alpha.cancel_by_tag("tasks", "to-cancel") catch {
      e => {
        println("  cancel error: \{e}")
        0
      }
    }
    println("  cancel_by_tag cancelled \{n_cancelled}/\{nc} jobs")

    // Give the fetch loops one full XREADGROUP cycle (1000 ms) + buffer to
    // confirm no SlowTask jobs slip through.
    @async.sleep(1500)

    println("  SlowTask executions (should be 0): \{cancel_executed.val}")
    if n_cancelled == nc && cancel_executed.val == 0 {
      pass("all \{nc} jobs cancelled before execution; no SlowTask worker ran")
    } else if cancel_executed.val == 0 {
      // Partial cancel is still a win for the propagation proof.
      pass(
        "\{n_cancelled}/\{nc} cancelled at the API level; no SlowTask worker ran",
      )
    } else {
      fail(
        "\{cancel_executed.val} SlowTask job(s) executed despite cancellation (race: were already in PEL)",
      )
    }

    // ── Teardown ─────────────────────────────────────────────────────────────
    println("\n=== stopping both nodes ===")
    alpha.stop()
    beta.stop()
    println("done.")
  })
}
