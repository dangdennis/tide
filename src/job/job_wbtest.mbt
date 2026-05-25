// White-box tests for package-private serialization helpers.
// escape_json_string and parse_json_string_array are private; these tests
// must live in a _wbtest.mbt file to access them.

// ── escape_json_string ────────────────────────────────────────────────────────

///|
test "escape_json_string: embedded double-quote becomes backslash-quote" {
  assert_eq(escape_json_string("say \"hello\""), "say \\\"hello\\\"")
}

///|
test "escape_json_string: backslash becomes double-backslash" {
  assert_eq(escape_json_string("path\\file"), "path\\\\file")
}

///|
test "escape_json_string: newline becomes literal backslash-n sequence" {
  assert_eq(escape_json_string("line1\nline2"), "line1\\nline2")
}

///|
test "escape_json_string: carriage-return becomes literal backslash-r sequence" {
  assert_eq(escape_json_string("a\rb"), "a\\rb")
}

///|
test "escape_json_string: all special chars combined escape correctly" {
  let input = "say \"hello\"\npath\\file\r"
  let expected = "say \\\"hello\\\"\\npath\\\\file\\r"
  assert_eq(escape_json_string(input), expected)
}

///|
test "escape_json_string: empty string returns empty string" {
  assert_eq(escape_json_string(""), "")
}

///|
test "escape_json_string: plain string with no specials is unchanged" {
  assert_eq(escape_json_string("hello world"), "hello world")
}

// ── json_string_array / parse_json_string_array round-trips ──────────────────

///|
test "json_string_array / parse round-trip: string with embedded double-quote" {
  let original = ["say \"hello\""]
  let decoded = parse_json_string_array(json_string_array(original))
  assert_eq(decoded.length(), 1)
  assert_eq(decoded[0], original[0])
}

///|
test "json_string_array / parse round-trip: string with backslash" {
  let original = ["C:\\Users\\file"]
  let decoded = parse_json_string_array(json_string_array(original))
  assert_eq(decoded.length(), 1)
  assert_eq(decoded[0], original[0])
}

///|
test "json_string_array / parse round-trip: string with newline" {
  let original = ["line1\nline2"]
  let decoded = parse_json_string_array(json_string_array(original))
  assert_eq(decoded.length(), 1)
  assert_eq(decoded[0], original[0])
}

///|
test "json_string_array / parse round-trip: multiple strings with mixed specials" {
  let original = ["say \"hi\"", "path\\dir", "a\nb"]
  let decoded = parse_json_string_array(json_string_array(original))
  assert_eq(decoded.length(), 3)
  assert_eq(decoded[0], original[0])
  assert_eq(decoded[1], original[1])
  assert_eq(decoded[2], original[2])
}

///|
test "json_string_array / parse round-trip: empty string element" {
  let original = [""]
  let decoded = parse_json_string_array(json_string_array(original))
  assert_eq(decoded.length(), 1)
  assert_eq(decoded[0], "")
}

///|
test "json_string_array / parse round-trip: empty array" {
  let original : Array[String] = []
  let decoded = parse_json_string_array(json_string_array(original))
  assert_eq(decoded.length(), 0)
}
