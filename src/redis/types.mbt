// RESP value types. tide speaks RESP2 by default and negotiates RESP3 on
// connect (HELLO 3) for cleaner push / pub-sub framing, falling back to RESP2
// against older servers. The extra RESP3 variants below are additive: callers
// that only know RESP2 keep working because aggregate accessors treat the new
// aggregate kinds (Map/Set/Push) like Array.

///|
pub(all) enum RespValue {
  SimpleStr(String)
  SimpleErr(String)
  Integer(Int64)
  BulkStr(String)
  Null
  Array(Array[RespValue])
  // ── RESP3 additions ──────────────────────────────────────────────────────
  Double(Double) //          ',' frame
  Boolean(Bool) //           '#' frame
  BigNumber(String) //       '(' frame (kept as text — no bignum dependency)
  Map(Array[RespValue]) //   '%' frame, flattened [k, v, k, v, ...]
  Set(Array[RespValue]) //   '~' frame
  Push(Array[RespValue]) //  '>' frame (pub/sub + out-of-band server pushes)
} derive(Debug, Eq)

///|
pub impl Show for RespValue with fn output(self, logger) {
  let write_list = fn(name : String, items : Array[RespValue]) {
    logger.write_string(name)
    logger.write_string("([")
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
  match self {
    SimpleStr(s) => logger.write_string("SimpleStr(\"\{s}\")")
    SimpleErr(e) => logger.write_string("SimpleErr(\"\{e}\")")
    Integer(n) => logger.write_string("Integer(\{n})")
    BulkStr(s) => logger.write_string("BulkStr(\"\{s}\")")
    Null => logger.write_string("Null")
    Array(items) => write_list("Array", items)
    Double(n) => logger.write_string("Double(\{n})")
    Boolean(b) => logger.write_string("Boolean(\{b})")
    BigNumber(s) => logger.write_string("BigNumber(\"\{s}\")")
    Map(items) => write_list("Map", items)
    Set(items) => write_list("Set", items)
    Push(items) => write_list("Push", items)
  }
}

///|
pub suberror RedisError {
  ConnectionFailed(String)
  IoError(String)
  ParseError(String)
  CommandError(String) // Redis returned a -ERR response
  Timeout(String) // a connect or command deadline elapsed
  PoolExhausted
}

///|
pub impl Show for RedisError with fn output(self, logger) {
  match self {
    ConnectionFailed(msg) => logger.write_string("ConnectionFailed(\{msg})")
    IoError(msg) => logger.write_string("IoError(\{msg})")
    ParseError(msg) => logger.write_string("ParseError(\{msg})")
    CommandError(msg) => logger.write_string("CommandError(\{msg})")
    Timeout(msg) => logger.write_string("Timeout(\{msg})")
    PoolExhausted => logger.write_string("PoolExhausted")
  }
}

// Unwrap a RespValue as a String, or raise CommandError.

///|
pub fn RespValue::as_string(self : RespValue) -> String raise RedisError {
  match self {
    SimpleStr(s) | BulkStr(s) | BigNumber(s) => s
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
pub fn RespValue::as_array(self : RespValue) -> Array[RespValue] raise RedisError {
  match self {
    // RESP3 maps/sets/pushes carry their elements the same way arrays do, so
    // commands like HGETALL keep working whether the server replies with '*'
    // or with the RESP3 '%' map type.
    Array(arr) | Map(arr) | Set(arr) | Push(arr) => arr
    Null => []
    SimpleErr(e) => raise RedisError::CommandError(e)
    _ => raise RedisError::ParseError("expected array, got non-array value")
  }
}

///|
pub fn RespValue::as_bool(self : RespValue) -> Bool raise RedisError {
  match self {
    Boolean(b) => b
    Integer(n) => n != 0L
    SimpleErr(e) => raise RedisError::CommandError(e)
    _ => raise RedisError::ParseError("expected boolean, got incompatible value")
  }
}

///|
pub fn RespValue::as_double(self : RespValue) -> Double raise RedisError {
  match self {
    Double(d) => d
    Integer(n) => n.to_double()
    SimpleErr(e) => raise RedisError::CommandError(e)
    _ => raise RedisError::ParseError("expected double, got incompatible value")
  }
}

///|
pub fn RespValue::is_null(self : RespValue) -> Bool {
  match self {
    Null => true
    _ => false
  }
}
