// Tests for key-naming helpers and the find_field utility.

// ── Key format strings ────────────────────────────────────────────────────────

///|
test "job_hash_key format" {
  assert_eq(job_hash_key("abc-123"), "tide:job:abc-123")
}

///|
test "job_hash_key with empty id" {
  assert_eq(job_hash_key(""), "tide:job:")
}

///|
test "available_key format" {
  assert_eq(available_key("default"), "tide:{default}:available")
}

///|
test "available_key preserves queue name" {
  assert_eq(
    available_key("email-notifications"),
    "tide:{email-notifications}:available",
  )
}

///|
test "scheduled_key format" {
  assert_eq(scheduled_key("default"), "tide:{default}:scheduled")
}

///|
test "stream_key format" {
  assert_eq(stream_key("default"), "tide:{default}:stream")
}

///|
test "stream_key different queue" {
  assert_eq(stream_key("critical"), "tide:{critical}:stream")
}

// ── find_field ────────────────────────────────────────────────────────────────

///|
test "find_field returns value for existing key" {
  debug_inspect(find_field(["a", "1", "b", "2"], "b"), content="Some(\"2\")")
}

///|
test "find_field returns value for first key" {
  debug_inspect(find_field(["a", "1", "b", "2"], "a"), content="Some(\"1\")")
}

///|
test "find_field returns None for missing key" {
  debug_inspect(find_field(["a", "1", "b", "2"], "c"), content="None")
}

///|
test "find_field on empty array returns None" {
  debug_inspect(find_field([], "x"), content="None")
}

///|
test "find_field on single pair" {
  debug_inspect(find_field(["key", "val"], "key"), content="Some(\"val\")")
}

///|
test "find_field on odd-length array does not panic" {
  // The trailing unpaired key has no value to return.
  debug_inspect(find_field(["a", "1", "b"], "b"), content="None")
}

///|
test "find_field returns first match when key appears multiple times" {
  // Duplicates shouldn't happen in practice but the function returns the first.
  debug_inspect(find_field(["k", "v1", "k", "v2"], "k"), content="Some(\"v1\")")
}
