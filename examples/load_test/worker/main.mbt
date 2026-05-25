// load_test/worker — standalone tide worker process for multi-node load testing.
//
// Each instance of this process is a genuine OS-level node with its own Redis
// consumer-group membership and its own leader-election slot.  Two workers
// running in separate terminals will have different node_ids and will each
// claim a share of the benchmark queue.
//
// Usage:
//   Terminal 1:  moon run examples/load_test/worker --target native
//   Terminal 2:  moon run examples/load_test/worker --target native
//   Terminal 3:  moon run examples/load_test/producer --target native
//
// Prerequisites:  docker compose up -d redis
// Stop:           Ctrl+C in each worker terminal

///|
async fn main {
  println("=== tide load-test worker ===")
  println("Queue: benchmark | Concurrency: 25 | Stop: Ctrl+C\n")

  let config : @tide.Config = {
    ..@tide.Config::default(),
    queues: Map([("benchmark", 25)]),
    pool_size: 10,
    name: "load-test",
    peer_mode: @tide.PeerMode::Redis,
    notify_cancel: false,
    workers: Map([]),
  }

  let instance = @tide.Instance::start(config) catch {
    e => {
      println("ERROR starting instance: \{e}")
      panic()
    }
  }

  let node_id = instance.engine.node_id
  println("node_id: \{node_id}")
  println("leader:  \{instance.is_leader()}\n")

  let processed : Ref[Int] = Ref(0)
  let window_start : Ref[Int64] = Ref(@async.now())

  instance.register("BenchJob", fn(_job) {
    processed.val += 1
    if processed.val % 100 == 0 {
      let now = @async.now()
      let window_ms = now - window_start.val
      let rate = if window_ms > 0L {
        100L * 1000L / window_ms
      } else {
        0L
      }
      println(
        "  [\{node_id}]  done: \{processed.val}  last-100: ~\{rate} jobs/s",
      )
      window_start.val = now
    }
    @worker.PerformResult::Ok
  })

  println("worker ready — waiting for jobs …")
  instance.run() catch {
    e => println("run error: \{e}")
  }
}
