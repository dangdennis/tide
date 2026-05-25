// load_test/producer — insert N BenchJob jobs and measure multi-node drain.
//
// Metrics reported:
//   1. Insert throughput  — jobs written to Redis per second
//   2. Drain throughput   — end-to-end jobs/s while workers consume the queue
//   3. Node distribution  — samples attempted_by from job hashes to prove that
//                           multiple distinct worker processes handled the load
//
// How to run a multi-node load test:
//   1. docker compose up -d redis
//   2. Open two terminals and start a worker in each:
//        moon run examples/load_test/worker --target native
//   3. In a third terminal, run this producer:
//        moon run examples/load_test/producer --target native
//
// The drain monitor polls every 250 ms and prints a progress line whenever
// the queue depth or in-flight count changes.  When both reach 0 the
// per-node breakdown shows which node_ids handled which share of the sample.

///|
async fn main {
  println("=== tide load-test producer ===")
  println("Connecting to Redis at 127.0.0.1:6379 …\n")

  let n = 1000
  let queue = "benchmark"
  let redis_config = @redis.ClientConfig::default()

  // Minimal Instance — only used for job insertion.
  // We never call instance.run(), so no fetch loop starts here.
  let config : @tide.Config = {
    ..@tide.Config::default(),
    queues: Map([(queue, 1)]),
    pool_size: 5,
    name: "load-test",
    workers: Map([]),
  }

  let instance = @tide.Instance::start(config) catch {
    e => {
      println("ERROR starting instance: \{e}")
      panic()
    }
  }

  // ── Insert ───────────────────────────────────────────────────────────────

  let jobs : Array[@job.Job] = Array::new(capacity=n)
  for i in 0..<n {
    jobs.push(@job.Job::new(queue, "BenchJob").with_args("{\"i\":\{i}}"))
  }

  println("inserting \{n} BenchJob jobs …")
  let t_insert = @async.now()
  let inserted = instance.insert_all(jobs) catch {
    e => {
      println("ERROR inserting: \{e}")
      panic()
    }
  }
  let insert_ms = @async.now() - t_insert
  let insert_rate = if insert_ms > 0L {
    inserted.length().to_int64() * 1000L / insert_ms
  } else {
    0L
  }
  println(
    "inserted \{inserted.length()} jobs in \{insert_ms}ms  ≈ \{insert_rate} inserts/s\n",
  )

  // Collect 20 sample job IDs spread across the batch for post-drain analysis.
  let sample_ids : Array[String] = []
  let sample_count = 20
  if inserted.length() >= sample_count {
    let step = inserted.length() / sample_count
    for i in 0..<sample_count {
      sample_ids.push(inserted[i * step].id)
    }
  }

  // ── Drain monitor ─────────────────────────────────────────────────────────

  let pool = @redis.Pool::new(redis_config, 2) catch {
    e => {
      println("ERROR creating monitoring pool: \{e}")
      panic()
    }
  }

  let avail_key = "tide:{" + queue + "}:available"
  let strm_key = "tide:{" + queue + "}:stream"

  println("── Drain monitor ────────────────────────────────────────────────")
  println("  Polling every 250 ms until queue is empty …\n")

  let t_drain = @async.now()
  let mut prev_available = -1L
  let mut prev_pending = -1L

  while true {
    let (available, pending) = pool.with_conn(async fn(conn) {
      let av = conn.command(["ZCARD", avail_key]).as_int() catch { _ => -1L }
      let pend_arr = conn.command(["XPENDING", strm_key, "workers"]).as_array() catch {
        _ => []
      }
      let pend = if pend_arr.is_empty() {
        0L
      } else {
        pend_arr[0].as_int() catch { _ => 0L }
      }
      (av, pend)
    }) catch {
      _ => (-1L, 0L)
    }

    if available != prev_available || pending != prev_pending {
      let elapsed = @async.now() - t_drain
      let remaining = if available >= 0L { available + pending } else { -1L }
      println(
        "  t=+\{elapsed}ms  queued=\{available}  in-flight=\{pending}  remaining=\{remaining}",
      )
      prev_available = available
      prev_pending = pending
    }

    if available == 0L && pending == 0L {
      break
    }

    @async.sleep(250)
  }

  let drain_ms = @async.now() - t_drain
  let drain_rate = if drain_ms > 0L {
    n.to_int64() * 1000L / drain_ms
  } else {
    0L
  }

  println("\n── Results ──────────────────────────────────────────────────────")
  println("  total jobs:     \{n}")
  println("  insert time:    \{insert_ms}ms  (\{insert_rate} inserts/s)")
  println("  drain time:     \{drain_ms}ms  (\{drain_rate} jobs/s throughput)")

  // ── Node distribution ─────────────────────────────────────────────────────

  if !sample_ids.is_empty() {
    println(
      "\n── Node distribution (sampled \{sample_ids.length()} of \{n} jobs) ───────────────",
    )

    let seen_nodes : Array[String] = []
    let node_hits : Array[Int] = []

    pool.with_conn(async fn(conn) {
      for id in sample_ids {
        let raw = conn.hget("tide:job:\{id}", "attempted_by") catch { _ => None }
        match raw {
          None =>
            println(
              "  job \{id}  (no data — job may have been pruned or not yet processed)",
            )
          Some(s) => {
            let node = first_node_in(s)
            println("  job \{id}  →  \{node}")
            match find_index(seen_nodes, node) {
              None => {
                seen_nodes.push(node)
                node_hits.push(1)
              }
              Some(idx) => node_hits[idx] = node_hits[idx] + 1
            }
          }
        }
      }
    }) catch {
      _ => ()
    }

    println("\n  per-node totals:")
    for i in 0..<seen_nodes.length() {
      let pct = if sample_ids.length() > 0 {
        node_hits[i] * 100 / sample_ids.length()
      } else {
        0
      }
      println(
        "    \{seen_nodes[i]}  →  \{node_hits[i]}/\{sample_ids.length()} (\{pct}%)",
      )
    }

    if seen_nodes.length() > 1 {
      println(
        "\n  ✓ PASS  \{seen_nodes.length()} distinct nodes — genuine multi-node distribution",
      )
    } else if seen_nodes.length() == 1 {
      println(
        "\n  ~ INFO  all sampled jobs processed by one node — start a second worker for multi-node proof",
      )
    } else {
      println("\n  ✗ FAIL  no jobs were processed (are workers running?)")
    }
  }
}

// Return the first "node-..." ID found in a JSON array string such as
// ["node-1748123456789"] or ["node-123","node-456"].

///|
fn first_node_in(raw : String) -> String {
  let tag = "node-"
  match raw.find(tag) {
    None => raw
    Some(start) => {
      let mut end = start + tag.length()
      while end < raw.length() && raw[end] != '"' {
        end = end + 1
      }
      raw[start:end].to_owned()
    }
  }
}

// Return the index of the first occurrence of `target` in `arr`, or None.

///|
fn find_index(arr : Array[String], target : String) -> Int? {
  for i in 0..<arr.length() {
    if arr[i] == target {
      return Some(i)
    }
  }
  None
}
