// Tests for Job serialization, scoring, backoff, and JobState.

// ── JobState ─────────────────────────────────────────────────────────────────

///|
test "jobstate to_string / from_string round-trip for all variants" {
  let states = [
    JobState::Available,
    JobState::Scheduled,
    JobState::Executing,
    JobState::Completed,
    JobState::Retryable,
    JobState::Discarded,
    JobState::Cancelled,
  ]
  for state in states {
    match JobState::from_string(state.to_string()) {
      Some(s) => assert_eq(s, state)
      None => abort("from_string returned None for \{state.to_string()}")
    }
  }
}

///|
test "jobstate from_string unknown returns None" {
  debug_inspect(JobState::from_string("unknown"), content="None")
}

///|
test "jobstate from_string empty returns None" {
  debug_inspect(JobState::from_string(""), content="None")
}

// ── to_hash_fields ────────────────────────────────────────────────────────────

///|
test "to_hash_fields returns even-length array" {
  let job = Job::new("default", "EmailWorker")
  let fields = job.to_hash_fields()
  assert_eq(fields.length() % 2, 0)
}

///|
test "to_hash_fields returns non-empty array" {
  let job = Job::new("default", "EmailWorker")
  assert_true(job.to_hash_fields().length() > 0)
}

// ── from_hash_fields ──────────────────────────────────────────────────────────

///|
test "from_hash_fields round-trip for core scalar fields" {
  let job : Job = {
    id: "test-id-123",
    queue: "default",
    worker: "EmailWorker",
    args: "{\"to\":\"user@example.com\"}",
    meta: "{}",
    errors: [],
    tags: [],
    state: JobState::Available,
    attempt: 2,
    max_attempts: 5,
    priority: 3,
    inserted_at: 1_000_000L,
    scheduled_at: 2_000_000L,
    attempted_at: None,
    completed_at: None,
    discarded_at: None,
    cancelled_at: None,
    attempted_by: [],
  }
  match Job::from_hash_fields(job.to_hash_fields()) {
    None => abort("round-trip failed: from_hash_fields returned None")
    Some(recovered) => {
      assert_eq(recovered.id, "test-id-123")
      assert_eq(recovered.queue, "default")
      assert_eq(recovered.worker, "EmailWorker")
      assert_eq(recovered.args, "{\"to\":\"user@example.com\"}")
      assert_eq(recovered.meta, "{}")
      assert_eq(recovered.state, JobState::Available)
      assert_eq(recovered.attempt, 2)
      assert_eq(recovered.max_attempts, 5)
      assert_eq(recovered.priority, 3)
      assert_eq(recovered.inserted_at, 1_000_000L)
      assert_eq(recovered.scheduled_at, 2_000_000L)
      debug_inspect(recovered.attempted_at, content="None")
      debug_inspect(recovered.completed_at, content="None")
      debug_inspect(recovered.discarded_at, content="None")
      debug_inspect(recovered.cancelled_at, content="None")
    }
  }
}

///|
test "from_hash_fields round-trip with optional timestamps set" {
  let job : Job = {
    id: "j1",
    queue: "q",
    worker: "W",
    args: "{}",
    meta: "{}",
    errors: [],
    tags: [],
    state: JobState::Completed,
    attempt: 1,
    max_attempts: 3,
    priority: 5,
    inserted_at: 100L,
    scheduled_at: 200L,
    attempted_at: Some(300L),
    completed_at: Some(400L),
    discarded_at: None,
    cancelled_at: None,
    attempted_by: [],
  }
  match Job::from_hash_fields(job.to_hash_fields()) {
    None => abort("round-trip failed")
    Some(recovered) => {
      assert_eq(recovered.state, JobState::Completed)
      debug_inspect(recovered.attempted_at, content="Some(300)")
      debug_inspect(recovered.completed_at, content="Some(400)")
    }
  }
}

///|
test "from_hash_fields returns None when id missing" {
  let fields = ["queue", "default", "worker", "W"]
  match Job::from_hash_fields(fields) {
    None => ()
    Some(_) => abort("expected None when id is missing")
  }
}

///|
test "from_hash_fields returns None when queue missing" {
  let fields = ["id", "j1", "worker", "W"]
  match Job::from_hash_fields(fields) {
    None => ()
    Some(_) => abort("expected None when queue is missing")
  }
}

///|
test "from_hash_fields returns None when state is invalid string" {
  let fields = [
    "id", "j1", "queue", "q", "worker", "w", "state", "bogus", "attempt", "0", "max_attempts",
    "3", "priority", "5", "inserted_at", "0", "scheduled_at", "0",
  ]
  match Job::from_hash_fields(fields) {
    None => ()
    Some(_) => abort("expected None for invalid state")
  }
}

///|
test "from_hash_fields returns None when attempt is not numeric" {
  let fields = [
    "id", "j1", "queue", "q", "worker", "w", "state", "available", "attempt", "notanumber",
    "max_attempts", "3", "priority", "5", "inserted_at", "0", "scheduled_at", "0",
  ]
  match Job::from_hash_fields(fields) {
    None => ()
    Some(_) => abort("expected None for non-numeric attempt")
  }
}

///|
test "from_hash_fields on empty array returns None" {
  match Job::from_hash_fields([]) {
    None => ()
    Some(_) => abort("expected None for empty input")
  }
}

// ── sorted_set_score ──────────────────────────────────────────────────────────

///|
test "sorted_set_score: lower priority integer scores lower than higher" {
  let base = Job::new("q", "W")
  let high = { ..base, priority: 0, scheduled_at: 1000L }
  let low = { ..base, priority: 9, scheduled_at: 1000L }
  assert_true(high.sorted_set_score() < low.sorted_set_score())
}

///|
test "sorted_set_score: earlier scheduled_at scores lower at same priority" {
  let base = Job::new("q", "W")
  let earlier = { ..base, priority: 5, scheduled_at: 1000L }
  let later = { ..base, priority: 5, scheduled_at: 2000L }
  assert_true(earlier.sorted_set_score() < later.sorted_set_score())
}

///|
test "sorted_set_score: same priority and scheduled_at produce equal scores" {
  let base = Job::new("q", "W")
  let a = { ..base, priority: 3, scheduled_at: 500L }
  let b = { ..base, priority: 3, scheduled_at: 500L }
  assert_eq(a.sorted_set_score(), b.sorted_set_score())
}

// ── default_backoff_ms ────────────────────────────────────────────────────────

///|
test "default_backoff_ms attempt 0 is 15 seconds" {
  assert_eq(default_backoff_ms(0), 15_000L)
}

///|
test "default_backoff_ms attempt 1 is 16 seconds" {
  assert_eq(default_backoff_ms(1), 16_000L)
}

///|
test "default_backoff_ms attempt 5 is 640 seconds" {
  assert_eq(default_backoff_ms(5), 640_000L)
}

///|
test "default_backoff_ms is capped at 86400 seconds" {
  assert_eq(default_backoff_ms(20), 86_400_000L)
  assert_eq(default_backoff_ms(100), 86_400_000L)
}

///|
test "default_backoff_ms grows with attempt" {
  assert_true(default_backoff_ms(1) > default_backoff_ms(0))
  assert_true(default_backoff_ms(3) > default_backoff_ms(2))
  assert_true(default_backoff_ms(5) > default_backoff_ms(4))
}

///|
test "default_backoff_ms attempt 17 is below the cap (83536 s)" {
  // 17^4 + 15 = 83521 + 15 = 83536 < 86400
  assert_eq(default_backoff_ms(17), 83_536_000L)
}

///|
test "default_backoff_ms attempt 18 hits the cap" {
  // 18^4 + 15 = 104976 + 15 = 104991 > 86400, so capped at 86400 s
  assert_eq(default_backoff_ms(18), 86_400_000L)
}

///|
test "default_backoff_ms overflow guard: attempt 216 returns the cap" {
  // 216^4 overflows 32-bit Int; the guard short-circuits to 86400 s
  assert_eq(default_backoff_ms(216), 86_400_000L)
}

// ── json_string_array output format (public API) ──────────────────────────────

///|
test "json_string_array produces well-formed JSON array of strings" {
  assert_eq(json_string_array(["a", "b"]), "[\"a\",\"b\"]")
}

///|
test "json_string_array empty array produces empty JSON array" {
  assert_eq(json_string_array([]), "[]")
}
