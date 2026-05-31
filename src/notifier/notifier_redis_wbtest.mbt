// Integration tests for the pub/sub cancel signal path.
// Requires: docker-compose up -d redis  (Redis on 127.0.0.1:6379)
// Run:      moon test --target native

///|
let notifier_test_url = "redis://127.0.0.1:6379"

///|
async test "listen exits cleanly when is_stopped is true from the start" {
  let config = @redis.ClientConfig::from_url(notifier_test_url) catch {
    _ => return
  }
  let notifier = RedisNotifier::new(config, "test:notifier:stop")
  let stopped = Ref(true)
  // listen() should open the connection, subscribe, immediately see is_stopped=true,
  // and return without processing any messages.
  notifier.listen(fn() { stopped.val })
}

///|
async test "publish_cancel → listen → mark_cancelled round-trip" {
  let config = @redis.ClientConfig::from_url(notifier_test_url) catch {
    _ => return
  }
  // Pool::new with size=1 eagerly opens a connection; skip if Redis is unavailable.
  let pool = @redis.Pool::new(config, 1) catch { _ => return }
  let notifier = RedisNotifier::new(config, "test:notifier:cancel")
  let stopped = Ref(false)
  @async.with_task_group(async fn(tg) {
    tg.spawn_bg(allow_failure=true) <| () => {
      notifier.listen(fn() { stopped.val })
    }
    // Give the subscriber connection time to register with Redis.
    @async.sleep(200)
    notifier.publish_cancel(pool, "job-abc-123")
    // Sleep long enough for the listener to receive and process the message.
    // listen() polls with a 1 s read timeout, so the message arrives well
    // within 500 ms since it is delivered immediately after publish.
    @async.sleep(500)
    stopped.val = true
    // with_task_group waits here for the listener task to exit (≤ 1100 ms more).
  })
  assert_true(notifier.is_cancelled("job-abc-123"))
}
