// Tests for key-naming helpers and the find_field utility.

// ── Key format strings ────────────────────────────────────────────────────────

test "job_hash_key format" {
  assert_eq!(job_hash_key("abc-123"), "tide:job:abc-123")
}

test "job_hash_key with empty id" {
  assert_eq!(job_hash_key(""), "tide:job:")
}

test "available_key format" {
  assert_eq!(available_key("default"), "tide:{default}:available")
}

test "available_key preserves queue name" {
  assert_eq!(available_key("email-notifications"), "tide:{email-notifications}:available")
}

test "scheduled_key format" {
  assert_eq!(scheduled_key("default"), "tide:{default}:scheduled")
}

test "stream_key format" {
  assert_eq!(stream_key("default"), "tide:{default}:stream")
}

test "stream_key different queue" {
  assert_eq!(stream_key("critical"), "tide:{critical}:stream")
}

// ── find_field ────────────────────────────────────────────────────────────────

test "find_field returns value for existing key" {
  assert_eq!(find_field(["a", "1", "b", "2"], "b"), Some("2"))
}

test "find_field returns value for first key" {
  assert_eq!(find_field(["a", "1", "b", "2"], "a"), Some("1"))
}

test "find_field returns None for missing key" {
  assert_eq!(find_field(["a", "1", "b", "2"], "c"), None)
}

test "find_field on empty array returns None" {
  assert_eq!(find_field([], "x"), None)
}

test "find_field on single pair" {
  assert_eq!(find_field(["key", "val"], "key"), Some("val"))
}

test "find_field on odd-length array does not panic" {
  // The trailing unpaired key has no value to return.
  assert_eq!(find_field(["a", "1", "b"], "b"), None)
}

test "find_field returns first match when key appears multiple times" {
  // Duplicates shouldn't happen in practice but the function returns the first.
  assert_eq!(find_field(["k", "v1", "k", "v2"], "k"), Some("v1"))
}
