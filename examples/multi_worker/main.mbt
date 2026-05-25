// multi_worker — tide example with multiple queues and worker types.
//
// Three queues, four workers.  Demonstrates:
//   • Multiple named queues with different concurrency limits
//   • Worker priority  (AlertEmailJob jumps ahead of WelcomeEmailJob)
//   • Job tags         (WelcomeEmailJob tagged ["onboarding"])
//   • Scheduled jobs   (ReportJob delayed 3 s; the stager promotes them)
//   • max_attempts     (ReportJob is single-attempt only)
//
// Prerequisites:
//   docker-compose up -d redis
//
// Run from the project root:
//   moon run examples/multi_worker --target native

///|
async fn main {
  println("[example] multi_worker starting")
  println("[example] connecting to Redis on 127.0.0.1:6379 …")

  // 3 immediate + 4 immediate + 3 immediate = 10 now, 2 scheduled (+3 s)
  let immediate = 10
  let scheduled = 2
  let total = immediate + scheduled

  let remaining : Ref[Int] = Ref(total)
  let all_done = @cond_var.Cond()

  // ── Configure three queues ───────────────────────────────────────────────
  let config = {
    ..@tide.Config::default(),
    queues: Map([("notifications", 5), ("media", 2), ("low_priority", 1)]),
    pool_size: 8,
    name: "multi-worker-example",
    shutdown_grace_ms: 5000,
  }

  let instance = @tide.Instance::start(config) catch {
    e => {
      println("ERROR: could not start tide: \{e}")
      panic()
    }
  }

  // ── Shared completion helper ─────────────────────────────────────────────
  // MoonBit's cooperative scheduler makes Ref[Int] safe across workers.
  let finish = fn(label : String, job : @job.Job) -> @worker.PerformResult {
    println("[worker] \{label}  id=\{job.id}  args=\{job.args}")
    remaining.val = remaining.val - 1
    if remaining.val == 0 {
      all_done.signal()
    }
    @worker.PerformResult::Ok
  }

  // ── Register workers ─────────────────────────────────────────────────────

  // "notifications" queue — two email worker types at different priorities.
  // AlertEmailJob is enqueued at priority=1, so it runs before WelcomeEmailJob
  // even if the welcome jobs were inserted first.
  instance.register("AlertEmailJob", fn(job) { finish("AlertEmail  ", job) })
  instance.register("WelcomeEmailJob", fn(job) { finish("WelcomeEmail", job) })

  // "media" queue — thumbnail resizer.
  instance.register("ThumbnailJob", fn(job) { finish("Thumbnail   ", job) })

  // "low_priority" queue — heavyweight reports.
  // max_attempts=1 means they are discarded (not retried) on failure.
  instance.register("ReportJob", fn(job) { finish("Report      ", job) })

  // ── Enqueue jobs ─────────────────────────────────────────────────────────
  let now = @async.now()

  // 3 alert emails — high priority.
  for i in 0..<3 {
    let job = @job.Job::new("notifications", "AlertEmailJob")
      .with_args("{\"to\":\"ops-\{i + 1}@example.com\"}")
      .with_priority(1)
    let inserted = instance.insert(job) catch {
      e => {
        println("ERROR: insert failed: \{e}")
        panic()
      }
    }
    println("[main]   enqueued AlertEmailJob    \{inserted.id}")
  }

  // 4 welcome emails — default priority, tagged for the onboarding pipeline.
  for i in 0..<4 {
    let job = @job.Job::new("notifications", "WelcomeEmailJob")
      .with_args("{\"to\":\"user-\{i + 1}@example.com\"}")
      .with_tags(["onboarding"])
    let inserted = instance.insert(job) catch {
      e => {
        println("ERROR: insert failed: \{e}")
        panic()
      }
    }
    println("[main]   enqueued WelcomeEmailJob  \{inserted.id}")
  }

  // 3 thumbnail jobs.
  for i in 0..<3 {
    let job = @job.Job::new("media", "ThumbnailJob").with_args(
      "{\"url\":\"https://cdn.example.com/img/\{i + 1}.jpg\"}",
    )
    let inserted = instance.insert(job) catch {
      e => {
        println("ERROR: insert failed: \{e}")
        panic()
      }
    }
    println("[main]   enqueued ThumbnailJob     \{inserted.id}")
  }

  // 2 report jobs scheduled 3 s from now.
  // They start in "scheduled" state; the stager loop promotes them to
  // "available" once their scheduled_at timestamp is reached.
  for i in 0..<2 {
    let job = @job.Job::new("low_priority", "ReportJob")
      .with_args("{\"report\":\{i + 1}}")
      .with_scheduled_at(now + 3000L)
      .with_max_attempts(1)
      .with_priority(8)
    let inserted = instance.insert(job) catch {
      e => {
        println("ERROR: insert failed: \{e}")
        panic()
      }
    }
    println("[main]   enqueued ReportJob +3 s   \{inserted.id}")
  }

  println(
    "[main]   \{total} jobs enqueued (\{immediate} immediate, \{scheduled} scheduled +3 s)",
  )

  // ── Run until all jobs complete (or 15 s timeout) ────────────────────────
  @async.with_task_group(async fn(tg) {
    // Tide processing loop in the background — includes the stager that
    // will promote the scheduled ReportJobs after 3 s.
    tg.spawn_bg(allow_failure=true) <| () => {
      instance.run() catch {
        _ => ()
      }
    }

    // Block until every worker has signalled, or 15 s elapses.
    let _ = @async.with_timeout_opt(15000, async fn() { all_done.wait() })

    instance.stop()

    let done = total - remaining.val
    println("[main]   finished. \{done}/\{total} jobs completed.")
  })
}
