// Tests for UniqueConfig and compute_unique_key.

///|
test "compute_unique_key starts with tide:unique:" {
  let job = Job::new("default", "EmailWorker")
  let config = UniqueConfig::new(3_600_000L)
  assert_true(compute_unique_key(job, config).has_prefix("tide:unique:"))
}

///|
test "compute_unique_key default fields: queue + worker + args" {
  let job = Job::new("default", "EmailWorker")
  let config = UniqueConfig::new(3_600_000L)
  assert_eq(
    compute_unique_key(job, config),
    "tide:unique:default:EmailWorker:{}",
  )
}

///|
test "compute_unique_key is deterministic for the same job" {
  let job = Job::new("q", "W").with_args("{\"to\":\"user@example.com\"}")
  let config = UniqueConfig::new(3_600_000L)
  assert_eq(compute_unique_key(job, config), compute_unique_key(job, config))
}

///|
test "compute_unique_key differs when args differ" {
  let j1 = Job::new("q", "W").with_args("{\"to\":\"a@example.com\"}")
  let j2 = Job::new("q", "W").with_args("{\"to\":\"b@example.com\"}")
  let config = UniqueConfig::new(3_600_000L)
  assert_true(compute_unique_key(j1, config) != compute_unique_key(j2, config))
}

///|
test "compute_unique_key with queue+worker only ignores args" {
  let j1 = Job::new("q", "W").with_args("{\"x\":1}")
  let j2 = Job::new("q", "W").with_args("{\"x\":2}")
  let config = UniqueConfig::new(3_600_000L).with_fields(["queue", "worker"])
  assert_eq(compute_unique_key(j1, config), compute_unique_key(j2, config))
}

///|
test "compute_unique_key differs for different queues" {
  let j1 = Job::new("default", "W")
  let j2 = Job::new("critical", "W")
  let config = UniqueConfig::new(3_600_000L)
  assert_true(compute_unique_key(j1, config) != compute_unique_key(j2, config))
}

///|
test "compute_unique_key differs for different workers" {
  let j1 = Job::new("q", "WorkerA")
  let j2 = Job::new("q", "WorkerB")
  let config = UniqueConfig::new(3_600_000L)
  assert_true(compute_unique_key(j1, config) != compute_unique_key(j2, config))
}

///|
test "UniqueConfig::with_fields overrides the default field list" {
  let config = UniqueConfig::new(60_000L).with_fields(["queue"])
  assert_eq(config.fields.length(), 1)
  assert_eq(config.fields[0], "queue")
}

///|
test "compute_unique_key with empty fields produces a bare prefix key" {
  // Empty fields → key is "tide:unique:" with no segments.
  // All jobs with this config share the same lock regardless of queue/worker/args.
  // This is a footgun: document and test the expected (if surprising) behavior.
  let job = Job::new("q", "W")
  let config = UniqueConfig::new(60_000L).with_fields([])
  assert_eq(compute_unique_key(job, config), "tide:unique:")
}

///|
test "compute_unique_key with a single field includes only that field" {
  let j1 = Job::new("qA", "W")
  let j2 = Job::new("qB", "W")
  let config = UniqueConfig::new(60_000L).with_fields(["worker"])
  // Both jobs share the same worker, so the key must be identical.
  assert_eq(compute_unique_key(j1, config), compute_unique_key(j2, config))
  assert_eq(compute_unique_key(j1, config), "tide:unique:W")
}
