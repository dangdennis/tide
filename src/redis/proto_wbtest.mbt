// Whitebox tests for the byte-level RESP decoder: binary-safe framing, the
// incomplete-vs-error distinction, and RESP3 frame types. These live in a
// whitebox file so they can use the package's @utf8 import to build frames.

///|
fn frame(s : String) -> Bytes {
  @utf8.encode(s)
}

///|
test "bulk framing is byte-accurate for multi-byte UTF-8" {
  // "€" is 3 UTF-8 bytes but a single code unit's worth of text. The old
  // char-indexed decoder mis-sliced this; byte framing must recover it exactly.
  match decode_resp_bytes(frame("$3\r\n€\r\n"), 0) {
    Complete(val, next) => {
      assert_eq(val, RespValue::BulkStr("€"))
      assert_eq(next, 9) // 4 header + 3 body + 2 trailing CRLF
    }
    Incomplete => assert_true(false)
  }
}

///|
test "bulk framing is byte-accurate for a 4-byte code point" {
  match decode_resp_bytes(frame("$4\r\n😀\r\n"), 0) {
    Complete(val, _) => assert_eq(val, RespValue::BulkStr("😀"))
    Incomplete => assert_true(false)
  }
}

///|
test "truncated bulk is Incomplete, not an error" {
  match decode_resp_bytes(frame("$5\r\nhel"), 0) {
    Complete(_, _) => assert_true(false)
    Incomplete => assert_true(true)
  }
}

///|
test "simple string without CRLF is Incomplete" {
  match decode_resp_bytes(frame("+PARTIAL"), 0) {
    Complete(_, _) => assert_true(false)
    Incomplete => assert_true(true)
  }
}

///|
test "malformed input still raises (not Incomplete)" {
  let raised = try {
    let _ = decode_resp_bytes(frame("!oops\r\n"), 0)
    false
  } catch {
    RedisError::ParseError(_) => true
    _ => false
  }
  assert_true(raised)
}

///|
test "RESP3 double" {
  match decode_resp_bytes(frame(",3.14\r\n"), 0) {
    Complete(val, _) => assert_eq(val.as_double(), 3.14)
    Incomplete => assert_true(false)
  }
}

///|
test "RESP3 boolean" {
  match decode_resp_bytes(frame("#t\r\n"), 0) {
    Complete(val, _) => assert_eq(val, RespValue::Boolean(true))
    Incomplete => assert_true(false)
  }
  match decode_resp_bytes(frame("#f\r\n"), 0) {
    Complete(val, _) => assert_eq(val, RespValue::Boolean(false))
    Incomplete => assert_true(false)
  }
}

///|
test "RESP3 null" {
  match decode_resp_bytes(frame("_\r\n"), 0) {
    Complete(val, _) => assert_eq(val, RespValue::Null)
    Incomplete => assert_true(false)
  }
}

///|
test "RESP3 map decodes to flattened pairs" {
  let data = frame("%2\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n")
  match decode_resp_bytes(data, 0) {
    Complete(val, _) => {
      // as_array treats a Map like its flattened element list.
      assert_eq(val.as_array().length(), 4)
      assert_eq(val.as_array()[0], RespValue::BulkStr("a"))
    }
    Incomplete => assert_true(false)
  }
}

///|
test "RESP3 push frame" {
  let data = frame(">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n")
  match decode_resp_bytes(data, 0) {
    Complete(val, _) =>
      match val {
        Push(items) => {
          assert_eq(items.length(), 3)
          assert_eq(items[0], RespValue::BulkStr("message"))
        }
        _ => assert_true(false)
      }
    Incomplete => assert_true(false)
  }
}

///|
test "decode_resp String shim still raises on truncated bulk" {
  let raised = try {
    let _ = decode_resp("$5\r\nhi\r\n", 0)
    false
  } catch {
    _ => true
  }
  assert_true(raised)
}

// ── backoff_ms ────────────────────────────────────────────────────────────────

///|
test "backoff_ms: attempt 0 returns base delay" {
  let config = ClientConfig::default() // retry_base_delay_ms=50, retry_max_delay_ms=1000
  assert_eq(backoff_ms(config, 0), 50)
}

///|
test "backoff_ms: attempt 1 doubles the base" {
  let config = ClientConfig::default()
  assert_eq(backoff_ms(config, 1), 100)
}

///|
test "backoff_ms: attempt 2 doubles again" {
  let config = ClientConfig::default()
  assert_eq(backoff_ms(config, 2), 200)
}

///|
test "backoff_ms: attempt 5 is capped at retry_max_delay_ms" {
  let config = ClientConfig::default() // cap = 1000
  assert_eq(backoff_ms(config, 5), 1000)
}

///|
test "backoff_ms: custom config uses its own base and cap" {
  let config = {
    ..ClientConfig::default(),
    retry_base_delay_ms: 100,
    retry_max_delay_ms: 400,
  }
  assert_eq(backoff_ms(config, 0), 100) // base
  assert_eq(backoff_ms(config, 1), 200) // 100 * 2
  assert_eq(backoff_ms(config, 2), 400) // capped
  assert_eq(backoff_ms(config, 5), 400) // still capped
}
