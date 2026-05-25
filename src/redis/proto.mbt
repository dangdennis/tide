// RESP2 protocol encoder and decoder.

// Encode a Redis command as a RESP2 inline array of bulk strings.
// e.g. ["SET", "key", "value"] → "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
// Byte counts use arg.length() which is correct for ASCII arguments.

///|
pub fn encode_command(args : Array[String]) -> String {
  let sb = StringBuilder::new()
  sb.write_string("*")
  sb.write_string(args.length().to_string())
  sb.write_string("\r\n")
  for arg in args {
    sb.write_string("$")
    sb.write_string(@utf8.encode(arg).length().to_string())
    sb.write_string("\r\n")
    sb.write_string(arg)
    sb.write_string("\r\n")
  }
  sb.to_string()
}

// Decode one RESP2 value from `data` starting at `pos`.
// Returns (value, next_pos) so callers can chain multiple parses.

///|
pub fn decode_resp(
  data : String,
  pos : Int,
) -> (RespValue, Int) raise RedisError {
  if pos >= data.length() {
    raise RedisError::ParseError("unexpected end of input at pos \{pos}")
  }
  match data[pos] {
    '+' => {
      let (line, next) = read_line(data, pos + 1)
      (SimpleStr(line), next)
    }
    '-' => {
      let (line, next) = read_line(data, pos + 1)
      (SimpleErr(line), next)
    }
    ':' => {
      let (line, next) = read_line(data, pos + 1)
      match parse_int64(line) {
        Some(n) => (Integer(n), next)
        None => raise RedisError::ParseError("invalid integer: \{line}")
      }
    }
    '$' => {
      let (line, next) = read_line(data, pos + 1)
      match parse_int(line) {
        Some(-1) => (Null, next)
        Some(len) => {
          if next + len + 2 > data.length() {
            raise RedisError::ParseError("bulk string truncated at pos \{next}")
          }
          let value = data[next:next + len].to_owned()
          (BulkStr(value), next + len + 2) // skip trailing \r\n
        }
        None => raise RedisError::ParseError("invalid bulk length: \{line}")
      }
    }
    '*' => {
      let (line, next) = read_line(data, pos + 1)
      match parse_int(line) {
        Some(-1) => (Null, next)
        Some(count) => {
          let items : Array[RespValue] = Array::new(capacity=count)
          let mut cur = next
          for _ in 0..<count {
            proof_assert cur > pos && cur <= data.length()
            let (item, item_next) = decode_resp(data, cur)
            items.push(item)
            cur = item_next
          }
          (Array(items), cur)
        }
        None => raise RedisError::ParseError("invalid array length: \{line}")
      }
    }
    c => raise RedisError::ParseError("unknown RESP type byte: \{c}")
  }
}

// Read characters until \r\n; returns (content, pos_after_crlf).

///|
fn read_line(data : String, start : Int) -> (String, Int) raise RedisError {
  let mut i = start
  while i + 1 < data.length() {
    proof_assert i >= start && i < data.length()
    if data[i] == '\r' && data[i + 1] == '\n' {
      return (data[start:i].to_owned(), i + 2)
    }
    i = i + 1
  }
  raise RedisError::ParseError("no CRLF found starting at pos \{start}")
}

///|
fn parse_int(s : String) -> Int? {
  Some(@string.parse_int(s, base=10)) catch {
    _ => None
  }
}

///|
fn parse_int64(s : String) -> Int64? {
  Some(@string.parse_int64(s, base=10)) catch {
    _ => None
  }
}
