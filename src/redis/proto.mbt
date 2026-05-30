// RESP2/RESP3 protocol encoder and decoder.
//
// Decoding operates on raw bytes with byte-accurate framing so binary-safe
// payloads (e.g. job arguments containing non-ASCII or non-UTF-8 data) survive
// round-trips. The decoder distinguishes a genuinely malformed frame (raised as
// RedisError::ParseError) from a frame that is merely not fully buffered yet
// (returned as DecodeResult::Incomplete), so the connection read loop knows
// whether to read more bytes or surface a protocol error.

// Encode a Redis command as a RESP2 array of bulk strings.
// e.g. ["SET", "key", "value"] → "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
// Bulk lengths use the UTF-8 byte count, which is what the server expects.

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

///|
/// Outcome of decoding one RESP value from a byte buffer.
/// `Incomplete` means more bytes are needed to form a full frame; it is not an
/// error. Genuinely malformed input is raised as `RedisError::ParseError`.
pub enum DecodeResult {
  Complete(RespValue, Int) // decoded value + byte offset just past the frame
  Incomplete
}

///|
/// Decode one RESP2/RESP3 value from `data` starting at byte offset `pos`.
pub fn decode_resp_bytes(
  data : Bytes,
  pos : Int,
) -> DecodeResult raise RedisError {
  if pos >= data.length() {
    return Incomplete
  }
  let tag = data.at(pos)
  let start = pos + 1
  if tag == b'+' {
    match read_line_bytes(data, start) {
      Some((s, next)) => Complete(SimpleStr(s), next)
      None => Incomplete
    }
  } else if tag == b'-' {
    match read_line_bytes(data, start) {
      Some((s, next)) => Complete(SimpleErr(s), next)
      None => Incomplete
    }
  } else if tag == b':' {
    match read_line_bytes(data, start) {
      Some((s, next)) =>
        match parse_int64(s) {
          Some(n) => Complete(Integer(n), next)
          None => raise RedisError::ParseError("invalid integer: \{s}")
        }
      None => Incomplete
    }
  } else if tag == b',' {
    match read_line_bytes(data, start) {
      Some((s, next)) =>
        match parse_double(s) {
          Some(d) => Complete(Double(d), next)
          None => raise RedisError::ParseError("invalid double: \{s}")
        }
      None => Incomplete
    }
  } else if tag == b'#' {
    match read_line_bytes(data, start) {
      Some((s, next)) => Complete(Boolean(s == "t"), next)
      None => Incomplete
    }
  } else if tag == b'(' {
    match read_line_bytes(data, start) {
      Some((s, next)) => Complete(BigNumber(s), next)
      None => Incomplete
    }
  } else if tag == b'_' {
    // RESP3 null: "_\r\n"
    match read_line_bytes(data, start) {
      Some((_, next)) => Complete(Null, next)
      None => Incomplete
    }
  } else if tag == b'$' {
    decode_bulk(data, start, verbatim=false)
  } else if tag == b'=' {
    // RESP3 verbatim string: like a bulk string with a 3-char type + ':' prefix.
    decode_bulk(data, start, verbatim=true)
  } else if tag == b'*' || tag == b'~' || tag == b'>' {
    // array ('*'), set ('~'), push ('>') — same element framing.
    decode_aggregate(data, start, fn(items) {
      if tag == b'~' {
        Set(items)
      } else if tag == b'>' {
        Push(items)
      } else {
        Array(items)
      }
    })
  } else if tag == b'%' {
    // RESP3 map: <n> pairs => 2*n elements, flattened [k, v, k, v, ...].
    decode_pairs(data, start, fn(items) { Map(items) })
  } else if tag == b'|' {
    // RESP3 attribute: a metadata map followed by the actual value. We decode
    // and discard the metadata, then return the value that follows it.
    match read_line_bytes(data, start) {
      None => Incomplete
      Some((s, next)) =>
        match parse_int(s) {
          Some(count) =>
            if count < 0 {
              raise RedisError::ParseError("invalid attribute length: \{s}")
            } else {
              match decode_n(data, next, count * 2) {
                None => Incomplete
                Some((_, cur)) => decode_resp_bytes(data, cur)
              }
            }
          None => raise RedisError::ParseError("invalid attribute length: \{s}")
        }
    }
  } else {
    raise RedisError::ParseError("unknown RESP type byte: \{tag}")
  }
}

///|
/// Decode a bulk ('$') or verbatim ('=') string starting just after the tag.
fn decode_bulk(
  data : Bytes,
  start : Int,
  verbatim~ : Bool,
) -> DecodeResult raise RedisError {
  match read_line_bytes(data, start) {
    None => Incomplete
    Some((s, next)) =>
      match parse_int(s) {
        Some(-1) => Complete(Null, next)
        Some(len) =>
          if len < 0 {
            raise RedisError::ParseError("invalid bulk length: \{s}")
          } else if next + len + 2 > data.length() {
            Incomplete
          } else {
            // Verbatim strings carry a 4-byte "txt:"/"mkd:" prefix we strip.
            let body_start = if verbatim && len >= 4 { next + 4 } else { next }
            let value = @utf8.decode_lossy(data.view(start=body_start, end=next + len))
            Complete(BulkStr(value), next + len + 2)
          }
        None => raise RedisError::ParseError("invalid bulk length: \{s}")
      }
  }
}

///|
/// Decode an aggregate with `n` elements ('*'/'~'/'>').
fn decode_aggregate(
  data : Bytes,
  start : Int,
  make : (Array[RespValue]) -> RespValue,
) -> DecodeResult raise RedisError {
  match read_line_bytes(data, start) {
    None => Incomplete
    Some((s, next)) =>
      match parse_int(s) {
        Some(-1) => Complete(Null, next) // RESP2 null array
        Some(count) =>
          if count < 0 {
            raise RedisError::ParseError("invalid aggregate length: \{s}")
          } else {
            match decode_n(data, next, count) {
              None => Incomplete
              Some((items, cur)) => Complete(make(items), cur)
            }
          }
        None => raise RedisError::ParseError("invalid aggregate length: \{s}")
      }
  }
}

///|
/// Decode a map-like aggregate with `n` key/value pairs ('%').
fn decode_pairs(
  data : Bytes,
  start : Int,
  make : (Array[RespValue]) -> RespValue,
) -> DecodeResult raise RedisError {
  match read_line_bytes(data, start) {
    None => Incomplete
    Some((s, next)) =>
      match parse_int(s) {
        Some(count) =>
          if count < 0 {
            raise RedisError::ParseError("invalid map length: \{s}")
          } else {
            match decode_n(data, next, count * 2) {
              None => Incomplete
              Some((items, cur)) => Complete(make(items), cur)
            }
          }
        None => raise RedisError::ParseError("invalid map length: \{s}")
      }
  }
}

///|
/// Decode exactly `count` RESP values in sequence. Returns None (Incomplete) if
/// the buffer runs out partway through; raises on a malformed element.
fn decode_n(
  data : Bytes,
  start : Int,
  count : Int,
) -> (Array[RespValue], Int)? raise RedisError {
  let items : Array[RespValue] = Array::new(capacity=count)
  let mut cur = start
  for _ in 0..<count {
    match decode_resp_bytes(data, cur) {
      Complete(item, item_next) => {
        items.push(item)
        cur = item_next
      }
      Incomplete => return None
    }
  }
  Some((items, cur))
}

///|
/// Read bytes until CRLF starting at `start`. Returns the (UTF-8 decoded) line
/// content and the offset just past the CRLF, or None if no CRLF is buffered.
fn read_line_bytes(data : Bytes, start : Int) -> (String, Int)? {
  let n = data.length()
  let mut i = start
  while i + 1 < n {
    if data.at(i) == b'\r' && data.at(i + 1) == b'\n' {
      return Some((@utf8.decode_lossy(data.view(start=start, end=i)), i + 2))
    }
    i = i + 1
  }
  None
}

///|
/// Backwards-compatible String-based decode used by existing tests and callers.
/// Encodes the input to UTF-8 bytes and adapts `Incomplete` into a ParseError,
/// preserving the original "raise on a partial/empty frame" behaviour.
pub fn decode_resp(data : String, pos : Int) -> (RespValue, Int) raise RedisError {
  match decode_resp_bytes(@utf8.encode(data), pos) {
    Complete(v, next) => (v, next)
    Incomplete => raise RedisError::ParseError("incomplete RESP frame at pos \{pos}")
  }
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

///|
fn parse_double(s : String) -> Double? {
  Some(@string.parse_double(s)) catch {
    _ => None
  }
}
