// RESP2 value types. tide uses RESP2 for broad Redis 5+ compatibility.
///|
pub(all) enum RespValue {
  SimpleStr(String)
  SimpleErr(String)
  Integer(Int64)
  BulkStr(String)
  Null
  Array(Array[RespValue])
} derive(Debug, Eq)

pub impl Show for RespValue with output(self, logger) {
  match self {
    SimpleStr(s) => logger.write_string("SimpleStr(\"\{s}\")")
    SimpleErr(e) => logger.write_string("SimpleErr(\"\{e}\")")
    Integer(n) => logger.write_string("Integer(\{n})")
    BulkStr(s) => logger.write_string("BulkStr(\"\{s}\")")
    Null => logger.write_string("Null")
    Array(items) => {
      logger.write_string("Array([")
      let mut first = true
      for item in items {
        if !first {
          logger.write_string(", ")
        }
        logger.write_object(item)
        first = false
      }
      logger.write_string("])")
    }
  }
}

///|
pub suberror RedisError {
  ConnectionFailed(String)
  IoError(String)
  ParseError(String)
  CommandError(String) // Redis returned a -ERR response
  PoolExhausted
}

///|
pub impl Show for RedisError with fn output(self, logger) {
  match self {
    ConnectionFailed(msg) => logger.write_string("ConnectionFailed(\{msg})")
    IoError(msg) => logger.write_string("IoError(\{msg})")
    ParseError(msg) => logger.write_string("ParseError(\{msg})")
    CommandError(msg) => logger.write_string("CommandError(\{msg})")
    PoolExhausted => logger.write_string("PoolExhausted")
  }
}

// Unwrap a RespValue as a String, or raise CommandError.

///|
pub fn RespValue::as_string(self : RespValue) -> String raise RedisError {
  match self {
    SimpleStr(s) | BulkStr(s) => s
    SimpleErr(e) => raise RedisError::CommandError(e)
    Null => ""
    _ => raise RedisError::ParseError("expected string, got non-string value")
  }
}

///|
pub fn RespValue::as_int(self : RespValue) -> Int64 raise RedisError {
  match self {
    Integer(n) => n
    SimpleErr(e) => raise RedisError::CommandError(e)
    _ => raise RedisError::ParseError("expected integer, got non-integer value")
  }
}

///|
pub fn RespValue::as_array(
  self : RespValue,
) -> Array[RespValue] raise RedisError {
  match self {
    Array(arr) => arr
    Null => []
    SimpleErr(e) => raise RedisError::CommandError(e)
    _ => raise RedisError::ParseError("expected array, got non-array value")
  }
}

///|
pub fn RespValue::is_null(self : RespValue) -> Bool {
  match self {
    Null => true
    _ => false
  }
}
