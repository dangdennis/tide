// print_jobs — minimal tide example
//
// Enqueues 5 "PrintJob" tasks, runs a tide instance that processes them,
// and exits once every job has been acknowledged.
//
// Prerequisites:
//   docker-compose up -d redis
//
// Run from the project root:
//   moon run examples/print_jobs --target native

///|
async fn main {
  println("[example] print_jobs starting")
  println("[example] connecting to Redis on 127.0.0.1:6379 …")

  let total = 5

  // Countdown: decremented inside the worker each time a job finishes.
  let remaining : Ref[Int] = Ref(total)
  let all_done = @cond_var.Cond()

  // ── Configure tide ──────────────────────────────────────────────────────────
  // Start from defaults and override the fields we care about.
  let config = {
    ..@tide.Config::default(),
    queues: Map([("default", 3)]), // up to 3 concurrent workers on "default"
    pool_size: 4,
    name: "print-jobs-example",
    shutdown_grace_ms: 5000,
  }

  let instance = @tide.Instance::start(config) catch {
    e => {
      println("ERROR: could not start tide: \{e}")
      panic()
    }
  }

  // ── Register worker ─────────────────────────────────────────────────────────
  // The closure receives the full Job struct; use job.args for your payload.
  instance.register("PrintJob", fn(job) {
    println("[worker] processing job_id=\{job.id}  args=\{job.args}")
    remaining.val = remaining.val - 1
    if remaining.val == 0 {
      all_done.signal()
    }
    @worker.PerformResult::Ok
  })

  // ── Enqueue jobs ────────────────────────────────────────────────────────────
  for i in 0..<total {
    let n = i + 1
    let job = @job.Job::new("default", "PrintJob").with_args("{\"n\":\{n}}")
    let inserted = instance.insert(job) catch {
      e => {
        println("ERROR: insert failed: \{e}")
        panic()
      }
    }
    println("[main]   enqueued \{inserted.id}  (n=\{n})")
  }

  // ── Run until all jobs complete (or 10 s timeout) ───────────────────────────
  @async.with_task_group(async fn(tg) {
    // Spin up the tide processing loop in the background.
    tg.spawn_bg(allow_failure=true) <| () => {
      instance.run() catch {
        _ => ()
      }
    }

    // Block until every worker has signalled, or 10 s elapses.
    let _ = @async.with_timeout_opt(10000, async fn() { all_done.wait() })

    instance.stop()

    let done = total - remaining.val
    println("[main]   finished. \{done}/\{total} jobs completed.")
  })
}
