// Unit tests for RedisNotifier (no Redis required — only pure-logic tests).

///|
test "is_cancelled returns false when no signal has been received" {
  let n = RedisNotifier::new(@redis.ClientConfig::default(), "tide:notify:default")
  assert_false(n.is_cancelled("job-123"))
}

///|
test "mark_cancelled makes is_cancelled return true" {
  let n = RedisNotifier::new(@redis.ClientConfig::default(), "tide:notify:default")
  n.mark_cancelled("job-abc")
  assert_true(n.is_cancelled("job-abc"))
}

///|
test "mark_cancelled does not affect unrelated job IDs" {
  let n = RedisNotifier::new(@redis.ClientConfig::default(), "tide:notify:default")
  n.mark_cancelled("job-1")
  assert_false(n.is_cancelled("job-2"))
}

///|
test "mark_cancelled is idempotent" {
  let n = RedisNotifier::new(@redis.ClientConfig::default(), "tide:notify:default")
  n.mark_cancelled("job-x")
  n.mark_cancelled("job-x")
  assert_true(n.is_cancelled("job-x"))
}

///|
test "multiple jobs can be marked cancelled independently" {
  let n = RedisNotifier::new(@redis.ClientConfig::default(), "ch")
  n.mark_cancelled("a")
  n.mark_cancelled("b")
  assert_true(n.is_cancelled("a"))
  assert_true(n.is_cancelled("b"))
  assert_false(n.is_cancelled("c"))
}
