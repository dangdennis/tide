// Tests for the RESP2 encoder and decoder.

///|
test "encode empty command" {
  assert_eq(encode_command([]), "*0\r\n")
}

///|
test "encode single arg" {
  assert_eq(encode_command(["PING"]), "*1\r\n$4\r\nPING\r\n")
}

///|
test "encode SET key value" {
  assert_eq(
    encode_command(["SET", "k", "v"]),
    "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
  )
}

///|
test "encode result is never empty" {
  assert_true(encode_command([]).length() > 0)
  assert_true(encode_command(["X"]).length() > 0)
}

///|
test "decode simple string" {
  let (val, next) = decode_resp("+OK\r\n", 0)
  assert_eq(next, 5)
  assert_eq(val, RespValue::SimpleStr("OK"))
}

///|
test "decode simple string empty value" {
  let (val, next) = decode_resp("+\r\n", 0)
  assert_eq(next, 3)
  assert_eq(val, RespValue::SimpleStr(""))
}

///|
test "decode simple error" {
  let (val, next) = decode_resp("-ERR bad\r\n", 0)
  assert_eq(next, 10)
  assert_eq(val, RespValue::SimpleErr("ERR bad"))
}

///|
test "decode integer positive" {
  let (val, next) = decode_resp(":42\r\n", 0)
  assert_eq(next, 5)
  assert_eq(val, RespValue::Integer(42L))
}

///|
test "decode integer zero" {
  let (val, _) = decode_resp(":0\r\n", 0)
  assert_eq(val, RespValue::Integer(0L))
}

///|
test "decode integer negative" {
  let (val, _) = decode_resp(":-1\r\n", 0)
  assert_eq(val, RespValue::Integer(-1L))
}

///|
test "decode bulk string" {
  let (val, next) = decode_resp("$3\r\nfoo\r\n", 0)
  assert_eq(next, 9)
  assert_eq(val, RespValue::BulkStr("foo"))
}

///|
test "decode bulk string empty" {
  let (val, next) = decode_resp("$0\r\n\r\n", 0)
  assert_eq(next, 6)
  assert_eq(val, RespValue::BulkStr(""))
}

///|
test "decode null bulk string" {
  let (val, next) = decode_resp("$-1\r\n", 0)
  assert_eq(next, 5)
  assert_eq(val, RespValue::Null)
}

///|
test "decode array of integers" {
  let (val, next) = decode_resp("*2\r\n:1\r\n:2\r\n", 0)
  assert_eq(next, 12)
  assert_eq(
    val,
    RespValue::Array([RespValue::Integer(1L), RespValue::Integer(2L)]),
  )
}

///|
test "decode empty array" {
  let (val, next) = decode_resp("*0\r\n", 0)
  assert_eq(next, 4)
  assert_eq(val, RespValue::Array([]))
}

///|
test "decode null array" {
  let (val, next) = decode_resp("*-1\r\n", 0)
  assert_eq(next, 5)
  assert_eq(val, RespValue::Null)
}

///|
test "decode nested array" {
  let (val, next) = decode_resp("*1\r\n*1\r\n+hi\r\n", 0)
  assert_eq(next, 13)
  assert_eq(
    val,
    RespValue::Array([RespValue::Array([RespValue::SimpleStr("hi")])]),
  )
}

///|
test "decode at non-zero position" {
  let data = "+OK\r\n+PONG\r\n"
  let (_, mid) = decode_resp(data, 0)
  let (val2, _) = decode_resp(data, mid)
  assert_eq(val2, RespValue::SimpleStr("PONG"))
}

///|
test "encode-decode round-trip" {
  let args = ["SET", "mykey", "myvalue"]
  let encoded = encode_command(args)
  let (val, _) = decode_resp(encoded, 0)
  assert_eq(
    val,
    RespValue::Array([
      RespValue::BulkStr("SET"),
      RespValue::BulkStr("mykey"),
      RespValue::BulkStr("myvalue"),
    ]),
  )
}

///|
test "decode error on empty input" {
  let raised = try {
    let _ = decode_resp("", 0)
    false
  } catch {
    _ => true
  }
  assert_true(raised)
}

///|
test "decode error on truncated bulk string" {
  let raised = try {
    let _ = decode_resp("$5\r\nhi\r\n", 0)
    false
  } catch {
    _ => true
  }
  assert_true(raised)
}

///|
test "decode error on unknown type byte" {
  let raised = try {
    let _ = decode_resp("!3\r\nfoo\r\n", 0)
    false
  } catch {
    _ => true
  }
  assert_true(raised)
}

///|
test "decode error on missing CRLF" {
  let raised = try {
    let _ = decode_resp("+OK", 0)
    false
  } catch {
    _ => true
  }
  assert_true(raised)
}

///|
test "decode error on invalid integer" {
  let raised = try {
    let _ = decode_resp(":notanint\r\n", 0)
    false
  } catch {
    _ => true
  }
  assert_true(raised)
}

///|
test "decode error on invalid bulk length" {
  let raised = try {
    let _ = decode_resp("$abc\r\nhello\r\n", 0)
    false
  } catch {
    _ => true
  }
  assert_true(raised)
}

///|
test "decode made progress: next_pos > start_pos" {
  let (_, next) = decode_resp("+OK\r\n", 0)
  assert_true(next > 0)
}

///|
test "decode stays in bounds: next_pos <= data length" {
  let data = "+OK\r\n"
  let (_, next) = decode_resp(data, 0)
  assert_true(next <= data.length())
}

///|
test "encode uses UTF-8 byte length for non-ASCII args" {
  // "café" = 5 UTF-8 bytes (not 4 chars); "€5" = 4 bytes (not 2 chars).
  // A char-count encoder would produce $4 and $2 respectively.
  assert_eq(
    encode_command(["SET", "café", "€5"]),
    "*3\r\n$3\r\nSET\r\n$5\r\ncafé\r\n$4\r\n€5\r\n",
  )
}
