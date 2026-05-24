// Tests for RespValue accessor methods.

///|
test "as_string from SimpleStr" {
  assert_eq(RespValue::SimpleStr("hello").as_string(), "hello")
}

///|
test "as_string from BulkStr" {
  assert_eq(RespValue::BulkStr("world").as_string(), "world")
}

///|
test "as_string from Null returns empty string" {
  assert_eq(RespValue::Null.as_string(), "")
}

///|
test "as_string from SimpleErr raises CommandError" {
  let raised = try {
    let _ = RespValue::SimpleErr("ERR bad").as_string()
    false
  } catch {
    RedisError::CommandError(_) => true
    _ => false
  }
  assert_true(raised)
}

///|
test "as_string from Integer raises ParseError" {
  let raised = try {
    let _ = RespValue::Integer(1L).as_string()
    false
  } catch {
    RedisError::ParseError(_) => true
    _ => false
  }
  assert_true(raised)
}

///|
test "as_string from Array raises ParseError" {
  let raised = try {
    let _ = RespValue::Array([]).as_string()
    false
  } catch {
    RedisError::ParseError(_) => true
    _ => false
  }
  assert_true(raised)
}

///|
test "as_int from Integer positive" {
  assert_eq(RespValue::Integer(99L).as_int(), 99L)
}

///|
test "as_int from Integer negative" {
  assert_eq(RespValue::Integer(-5L).as_int(), -5L)
}

///|
test "as_int from Integer zero" {
  assert_eq(RespValue::Integer(0L).as_int(), 0L)
}

///|
test "as_int from SimpleErr raises CommandError" {
  let raised = try {
    let _ = RespValue::SimpleErr("ERR").as_int()
    false
  } catch {
    RedisError::CommandError(_) => true
    _ => false
  }
  assert_true(raised)
}

///|
test "as_int from non-integer raises ParseError" {
  let raised = try {
    let _ = RespValue::SimpleStr("ok").as_int()
    false
  } catch {
    RedisError::ParseError(_) => true
    _ => false
  }
  assert_true(raised)
}

///|
test "as_array from Array" {
  let inner = [RespValue::Integer(1L), RespValue::Integer(2L)]
  assert_eq(RespValue::Array(inner).as_array().length(), 2)
}

///|
test "as_array from Null returns empty array" {
  assert_eq(RespValue::Null.as_array().length(), 0)
}

///|
test "as_array from SimpleErr raises CommandError" {
  let raised = try {
    let _ = RespValue::SimpleErr("ERR").as_array()
    false
  } catch {
    RedisError::CommandError(_) => true
    _ => false
  }
  assert_true(raised)
}

///|
test "as_array from non-array raises ParseError" {
  let raised = try {
    let _ = RespValue::Integer(1L).as_array()
    false
  } catch {
    RedisError::ParseError(_) => true
    _ => false
  }
  assert_true(raised)
}

///|
test "is_null on Null" {
  assert_true(RespValue::Null.is_null())
}

///|
test "is_null on SimpleStr" {
  assert_eq(RespValue::SimpleStr("ok").is_null(), false)
}

///|
test "is_null on Integer" {
  assert_eq(RespValue::Integer(0L).is_null(), false)
}

///|
test "is_null on empty Array" {
  assert_eq(RespValue::Array([]).is_null(), false)
}
