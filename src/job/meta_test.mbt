// Tests for the flat-JSON meta helpers.

// ── meta_get_string ───────────────────────────────────────────────────────────

///|
test "meta_get_string returns None on empty meta" {
  debug_inspect(meta_get_string("{}", "key"), content="None")
}

///|
test "meta_get_string retrieves an existing string value" {
  let m = "{\"greeting\":\"hello\"}"
  debug_inspect(meta_get_string(m, "greeting"), content="Some(\"hello\")")
}

///|
test "meta_get_string returns None when key is absent" {
  let m = "{\"a\":\"1\"}"
  debug_inspect(meta_get_string(m, "b"), content="None")
}

///|
test "meta_get_string returns None when value is not a string" {
  let m = "{\"active\":true}"
  debug_inspect(meta_get_string(m, "active"), content="None")
}

// ── meta_set_string ───────────────────────────────────────────────────────────

///|
test "meta_set_string on empty meta" {
  assert_eq(meta_set_string("{}", "key", "value"), "{\"key\":\"value\"}")
}

///|
test "meta_set_string appends a second key" {
  let m = meta_set_string("{}", "a", "1")
  let m2 = meta_set_string(m, "b", "2")
  assert_eq(m2, "{\"a\":\"1\",\"b\":\"2\"}")
}

///|
test "meta_set_string replaces an existing key" {
  let m = meta_set_string("{}", "key", "old")
  let m2 = meta_set_string(m, "key", "new")
  assert_eq(m2, "{\"key\":\"new\"}")
}

///|
test "meta_set_string round-trips through meta_get_string" {
  let m = meta_set_string("{}", "greeting", "world")
  debug_inspect(meta_get_string(m, "greeting"), content="Some(\"world\")")
}

// ── meta_get_bool / meta_set_bool ─────────────────────────────────────────────

///|
test "meta_set_bool true on empty meta" {
  assert_eq(meta_set_bool("{}", "active", true), "{\"active\":true}")
}

///|
test "meta_set_bool false on empty meta" {
  assert_eq(meta_set_bool("{}", "active", false), "{\"active\":false}")
}

///|
test "meta_get_bool true round-trip" {
  let m = meta_set_bool("{}", "active", true)
  debug_inspect(meta_get_bool(m, "active"), content="Some(true)")
}

///|
test "meta_get_bool false round-trip" {
  let m = meta_set_bool("{}", "active", false)
  debug_inspect(meta_get_bool(m, "active"), content="Some(false)")
}

///|
test "meta_get_bool returns None for absent key" {
  debug_inspect(meta_get_bool("{}", "missing"), content="None")
}

///|
test "meta_set_bool replaces existing bool" {
  let m = meta_set_bool("{}", "flag", true)
  let m2 = meta_set_bool(m, "flag", false)
  debug_inspect(meta_get_bool(m2, "flag"), content="Some(false)")
}

// ── mixed usage ───────────────────────────────────────────────────────────────

///|
test "meta_set_string and meta_set_bool can coexist" {
  let m = meta_set_string("{}", "name", "alice")
  let m2 = meta_set_bool(m, "cron", true)
  debug_inspect(meta_get_string(m2, "name"), content="Some(\"alice\")")
  debug_inspect(meta_get_bool(m2, "cron"), content="Some(true)")
}
