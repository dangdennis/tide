// Redis client: one TCP connection, sends RESP2 commands, parses responses.

pub struct ClientConfig {
  host : String
  port : Int
  password : Option[String]
  db : Int
} derive(Debug)

pub fn ClientConfig::default() -> ClientConfig {
  { host: "127.0.0.1", port: 6379, password: None, db: 0 }
}

pub fn ClientConfig::from_url(url : String) -> ClientConfig raise RedisError {
  // Minimal redis:// URL parser.
  // Expected format: redis://[:password@]host[:port][/db]
  if !url.has_prefix("redis://") {
    raise RedisError::ConnectionFailed("invalid URL scheme: \{url}")
  }
  let rest = url[8:].to_owned()
  let mut host = "127.0.0.1"
  let mut port = 6379
  let mut password : Option[String] = None
  let mut db = 0
  let (hostpart, db_part) = split_once(rest, "/")
  if !db_part.is_empty() {
    match parse_int(db_part) {
      Some(n) => db = n
      None => ()
    }
  }
  let (auth_part, host_part) = split_once_rev(hostpart, "@")
  if !auth_part.is_empty() {
    let (_, pass) = split_once(auth_part, ":")
    password = Some(pass)
  }
  let (h, port_str) = split_once_rev(host_part, ":")
  if !h.is_empty() {
    host = h
    match parse_int(port_str) {
      Some(p) => port = p
      None => ()
    }
  } else {
    host = host_part
  }
  { host, port, password, db }
}

// A single Redis connection.
pub struct Connection {
  socket : @socket.Tcp
  config : ClientConfig
  mut recv_buf : String
}

pub async fn Connection::open(config : ClientConfig) -> Connection raise RedisError {
  let addr_str = "\{config.host}:\{config.port}"
  let addr = try {
    @socket.Addr::parse(addr_str)
  } catch {
    e => raise RedisError::ConnectionFailed("invalid address \{addr_str}: \{e}")
  }
  let socket = try {
    @socket.Tcp::connect(addr)
  } catch {
    e => raise RedisError::ConnectionFailed("connect \{addr_str}: \{e}")
  }
  let conn : Connection = { socket, config, recv_buf: "" }
  match config.password {
    Some(pass) => {
      let _ = conn.command(["AUTH", pass])
    }
    None => ()
  }
  if config.db != 0 {
    let _ = conn.command(["SELECT", config.db.to_string()])
  }
  conn
}

pub fn Connection::close(self : Connection) -> Unit {
  self.socket.close()
}

// Send a RESP2 command and receive one response.
pub async fn Connection::command(
  self : Connection,
  args : Array[String],
) -> RespValue raise RedisError {
  let frame = encode_command(args)
  try {
    self.socket.write(frame)
  } catch {
    e => raise RedisError::IoError("write failed: \{e}")
  }
  self.recv_response()
}

// Send a pipeline (multiple commands) and return all responses in order.
pub async fn Connection::pipeline(
  self : Connection,
  cmds : Array[Array[String]],
) -> Array[RespValue] raise RedisError {
  let mut buf = ""
  for cmd in cmds {
    buf = buf + encode_command(cmd)
  }
  try {
    self.socket.write(buf)
  } catch {
    e => raise RedisError::IoError("write failed: \{e}")
  }
  let results : Array[RespValue] = Array::new(capacity=cmds.length())
  for _ in cmds {
    results.push(self.recv_response())
  }
  results
}

// Read bytes from the socket until we have a complete RESP2 response.
async fn Connection::recv_response(self : Connection) -> RespValue raise RedisError {
  while true {
    if !self.recv_buf.is_empty() {
      match try_decode(self.recv_buf) {
        Some((value, consumed)) => {
          self.recv_buf = self.recv_buf[consumed:].to_owned()
          return value
        }
        None => ()
      }
    }
    let chunk = try {
      self.socket.read_some()
    } catch {
      e => raise RedisError::IoError("read failed: \{e}")
    }
    match chunk {
      None => raise RedisError::IoError("connection closed by server")
      Some(bytes) => self.recv_buf = self.recv_buf + bytes.to_unchecked_string()
    }
  }
  raise RedisError::IoError("unexpected recv loop exit")
}

fn try_decode(data : String) -> Option[(RespValue, Int)] {
  try {
    Some(decode_resp(data, 0))
  } catch {
    _ => None
  }
}

// ── Typed command helpers ──────────────────────────────────────────────────

pub async fn Connection::set(
  self : Connection,
  key : String,
  value : String,
) -> Unit raise RedisError {
  let _ = self.command(["SET", key, value])
}

pub async fn Connection::set_px(
  self : Connection,
  key : String,
  value : String,
  px : Int64,
) -> Unit raise RedisError {
  let _ = self.command(["SET", key, value, "PX", px.to_string()])
}

// SET key value NX PX ms — returns true if key was set, false if it already existed.
pub async fn Connection::set_nx_px(
  self : Connection,
  key : String,
  value : String,
  px : Int64,
) -> Bool raise RedisError {
  let resp = self.command(["SET", key, value, "NX", "PX", px.to_string()])
  match resp {
    Null => false
    _ => true
  }
}

pub async fn Connection::get(
  self : Connection,
  key : String,
) -> Option[String] raise RedisError {
  match self.command(["GET", key]) {
    Null => None
    v => Some(v.as_string())
  }
}

pub async fn Connection::del(self : Connection, key : String) -> Int64 raise RedisError {
  self.command(["DEL", key]).as_int()
}

pub async fn Connection::exists(self : Connection, key : String) -> Bool raise RedisError {
  self.command(["EXISTS", key]).as_int() > 0L
}

pub async fn Connection::hset(
  self : Connection,
  key : String,
  fields : Array[String],
) -> Int64 raise RedisError {
  let args : Array[String] = ["HSET", key]
  args.append(fields)
  self.command(args).as_int()
}

pub async fn Connection::hget(
  self : Connection,
  key : String,
  field : String,
) -> Option[String] raise RedisError {
  match self.command(["HGET", key, field]) {
    Null => None
    v => Some(v.as_string())
  }
}

pub async fn Connection::hgetall(
  self : Connection,
  key : String,
) -> Array[String] raise RedisError {
  self.command(["HGETALL", key]).as_array().map(fn(v) {
    try {
      v.as_string()
    } catch {
      _ => ""
    }
  })
}

pub async fn Connection::zadd(
  self : Connection,
  key : String,
  score : Int64,
  entry : String,
) -> Int64 raise RedisError {
  self.command(["ZADD", key, score.to_string(), entry]).as_int()
}

pub async fn Connection::zrem(
  self : Connection,
  key : String,
  entry : String,
) -> Int64 raise RedisError {
  self.command(["ZREM", key, entry]).as_int()
}

pub async fn Connection::zpopmin(
  self : Connection,
  key : String,
  count : Int,
) -> Array[String] raise RedisError {
  self.command(["ZPOPMIN", key, count.to_string()]).as_array().map(fn(v) {
    try {
      v.as_string()
    } catch {
      _ => ""
    }
  })
}

pub async fn Connection::xadd(
  self : Connection,
  key : String,
  fields : Array[String],
) -> String raise RedisError {
  let args : Array[String] = ["XADD", key, "*"]
  args.append(fields)
  self.command(args).as_string()
}

pub async fn Connection::xgroup_create_mkstream(
  self : Connection,
  key : String,
  group : String,
) -> Unit raise RedisError {
  // Ignore BUSYGROUP error — consumer group already exists.
  try {
    let _ = self.command(["XGROUP", "CREATE", key, group, "$", "MKSTREAM"])
  } catch {
    RedisError::CommandError(msg) =>
      if !msg.contains("BUSYGROUP") {
        raise RedisError::CommandError(msg)
      }
    e => raise e
  }
}

pub struct XReadGroupEntry {
  stream_id : String
  fields : Array[String]
} derive(Debug)

pub async fn Connection::xreadgroup(
  self : Connection,
  group : String,
  consumer : String,
  stream_key : String,
  count~ : Int,
  block_ms~ : Int,
) -> Array[XReadGroupEntry] raise RedisError {
  let resp = self.command([
    "XREADGROUP",
    "GROUP",
    group,
    consumer,
    "COUNT",
    count.to_string(),
    "BLOCK",
    block_ms.to_string(),
    "STREAMS",
    stream_key,
    ">",
  ])
  parse_xreadgroup_response(resp)
}

pub async fn Connection::xack(
  self : Connection,
  key : String,
  group : String,
  stream_id : String,
) -> Int64 raise RedisError {
  self.command(["XACK", key, group, stream_id]).as_int()
}

pub async fn Connection::xautoclaim(
  self : Connection,
  key : String,
  group : String,
  consumer : String,
  min_idle_ms : Int64,
) -> Array[XReadGroupEntry] raise RedisError {
  let resp = self.command([
    "XAUTOCLAIM",
    key,
    group,
    consumer,
    min_idle_ms.to_string(),
    "0-0",
  ])
  // XAUTOCLAIM response: [next-start-id, [[id, fields], ...], [deleted-ids]]
  let arr = resp.as_array()
  if arr.length() < 2 {
    return []
  }
  parse_stream_entries(arr[1].as_array())
}

pub async fn Connection::evalsha(
  self : Connection,
  sha : String,
  keys : Array[String],
  argv : Array[String],
) -> RespValue raise RedisError {
  let args : Array[String] = ["EVALSHA", sha, keys.length().to_string()]
  args.append(keys)
  args.append(argv)
  self.command(args)
}

pub async fn Connection::script_load(
  self : Connection,
  script : String,
) -> String raise RedisError {
  self.command(["SCRIPT", "LOAD", script]).as_string()
}

pub async fn Connection::publish(
  self : Connection,
  channel : String,
  message : String,
) -> Int64 raise RedisError {
  self.command(["PUBLISH", channel, message]).as_int()
}

// Parse XREADGROUP response: [[stream_key, [[id, [f, v, ...]], ...]]]
fn parse_xreadgroup_response(
  resp : RespValue,
) -> Array[XReadGroupEntry] raise RedisError {
  match resp {
    Null => []
    Array(streams) => {
      if streams.is_empty() {
        return []
      }
      let stream = streams[0].as_array()
      if stream.length() < 2 {
        return []
      }
      parse_stream_entries(stream[1].as_array())
    }
    _ => raise RedisError::ParseError("unexpected XREADGROUP response")
  }
}

fn parse_stream_entries(
  entries : Array[RespValue],
) -> Array[XReadGroupEntry] raise RedisError {
  let result : Array[XReadGroupEntry] = []
  for entry in entries {
    let pair = entry.as_array()
    if pair.length() < 2 {
      continue
    }
    let stream_id = pair[0].as_string()
    let fields = pair[1].as_array().map(fn(v) {
      try {
        v.as_string()
      } catch {
        _ => ""
      }
    })
    result.push({ stream_id, fields })
  }
  result
}

// ── URL parsing helpers ────────────────────────────────────────────────────

fn split_once(s : String, sep : String) -> (String, String) {
  match s.find(sep) {
    Some(idx) => (s[:idx].to_owned(), s[idx + sep.length():].to_owned())
    None => (s, "")
  }
}

fn split_once_rev(s : String, sep : String) -> (String, String) {
  match s.rev_find(sep) {
    Some(idx) => (s[:idx].to_owned(), s[idx + sep.length():].to_owned())
    None => ("", s)
  }
}
