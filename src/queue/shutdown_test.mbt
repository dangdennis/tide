// Unit tests for ShutdownSignal: one-shot cooperative shutdown coordination.

///|
test "ShutdownSignal starts in non-stopped state" {
  let sig = ShutdownSignal::new()
  assert_false(sig.is_stopped())
}

///|
test "ShutdownSignal::send transitions to stopped" {
  let sig = ShutdownSignal::new()
  sig.send()
  assert_true(sig.is_stopped())
}

///|
test "ShutdownSignal::send is idempotent" {
  let sig = ShutdownSignal::new()
  sig.send()
  sig.send()
  assert_true(sig.is_stopped())
}

///|
async test "ShutdownSignal::wait returns after send" {
  let sig = ShutdownSignal::new()
  let done = Ref(false)
  @async.with_task_group(async fn(tg) {
    tg.spawn_bg(allow_failure=true) <| () => {
      sig.wait()
      done.val = true
    }
    @async.sleep(10)
    sig.send()
    @async.sleep(50)
  })
  assert_true(done.val)
}

///|
async test "ShutdownSignal::wait returns immediately when already stopped" {
  let sig = ShutdownSignal::new()
  sig.send()
  // wait() must not block if send() was already called.
  let finished = Ref(false)
  let _ = @async.with_timeout_opt(200, async fn() {
    sig.wait()
    finished.val = true
  })
  assert_true(finished.val)
}
