// Blackbox unit tests — only test the public API (no Redis required).

// ── LocalPeer ─────────────────────────────────────────────────────────────────

///|
test "LocalPeer::is_leader always returns true" {
  let p = LocalPeer::new()
  assert_true(p.is_leader())
}

///|
test "LocalPeer::is_leader is stable across multiple calls" {
  let p = LocalPeer::new()
  assert_true(p.is_leader())
  assert_true(p.is_leader())
}
