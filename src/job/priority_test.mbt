// Tests for priority constants, validation, and clamping.

///|
test "PRIORITY_CRITICAL is 0 (highest urgency)" {
  assert_eq(PRIORITY_CRITICAL, 0)
}

///|
test "PRIORITY_NORMAL is 5 (default)" {
  assert_eq(PRIORITY_NORMAL, 5)
}

///|
test "PRIORITY_LOWEST is 9 (lowest urgency)" {
  assert_eq(PRIORITY_LOWEST, 9)
}

///|
test "constants are ordered lowest-to-highest urgency" {
  assert_true(PRIORITY_CRITICAL < PRIORITY_HIGHEST)
  assert_true(PRIORITY_HIGHEST < PRIORITY_HIGH)
  assert_true(PRIORITY_HIGH < PRIORITY_ABOVE_NORMAL)
  assert_true(PRIORITY_ABOVE_NORMAL < PRIORITY_NORMAL)
  assert_true(PRIORITY_NORMAL < PRIORITY_BELOW_NORMAL)
  assert_true(PRIORITY_BELOW_NORMAL < PRIORITY_LOW)
  assert_true(PRIORITY_LOW < PRIORITY_LOWEST)
}

///|
test "priority_valid accepts boundary values 0 and 9" {
  assert_true(priority_valid(0))
  assert_true(priority_valid(9))
}

///|
test "priority_valid accepts mid-range values" {
  assert_true(priority_valid(1))
  assert_true(priority_valid(5))
  assert_true(priority_valid(8))
}

///|
test "priority_valid rejects negative values" {
  assert_false(priority_valid(-1))
  assert_false(priority_valid(-100))
}

///|
test "priority_valid rejects values above 9" {
  assert_false(priority_valid(10))
  assert_false(priority_valid(100))
}

///|
test "with_priority_clamped keeps value in valid range" {
  let job = Job::new("q", "W").with_priority_clamped(3)
  assert_eq(job.priority, 3)
}

///|
test "with_priority_clamped clamps negative to 0" {
  let job = Job::new("q", "W").with_priority_clamped(-5)
  assert_eq(job.priority, 0)
}

///|
test "with_priority_clamped clamps above 9 to 9" {
  let job = Job::new("q", "W").with_priority_clamped(15)
  assert_eq(job.priority, 9)
}

///|
test "with_priority_clamped preserves 0" {
  let job = Job::new("q", "W").with_priority_clamped(0)
  assert_eq(job.priority, 0)
}
