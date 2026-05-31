// Redis client: one TCP connection that sends RESP commands and parses
// responses. The receive buffer is binary-safe (raw bytes, not lossy-decoded
// UTF-8) and frames are byte-accurate. Connect and non-blocking commands honour
// configurable deadlines, and connect failures are retried with exponential
// backoff. Blocking commands (XREADGROUP BLOCK, pub/sub reads) deliberately
// bypass the per-command deadline.

///|
pub struct ClientConfig {
  host : String
  port : Int
  password : String?
  db : Int
  // Robustness knobs (all have sensible defaults via `default()`):
  connect_timeout_ms : Int // 0 disables the connect deadline
  command_timeout_ms : Int // 0 disables the per-command deadline
  max_retries : Int // extra connect attempts after the first
  retry_base_delay_ms : Int // backoff base; doubles each retry
  retry_max_delay_ms : Int // backoff cap
  enable_resp3 : Bool // negotiate HELLO 3, falling back to RESP2
} derive(Debug)

///|
pub fn ClientConfig::default() -> ClientConfig {
  {
    host: "127.0.0.1",
    port: 6379,
    password: None,
    db: 0,
    connect_timeout_ms: 5000,
    command_timeout_ms: 5000,
    max_retries: 2,
    retry_base_delay_ms: 50,
    retry_max_delay_ms: 1000,
    enable_resp3: true,
  }
}

///|
pub fn ClientConfig::from_url(url : String) -> ClientConfig raise RedisError {
  // Minimal redis:// URL parser.
  // Expected format: redis://[:password@]host[:port][/db]
  if !url.has_prefix("redis://") {
    raise RedisError::ConnectionFailed("invalid URL scheme: \{url}")
  }
  let rest = url[8:].to_owned()
  let mut host = "127.0.0.1"
  let mut port = 6379
  let mut password : String? = None
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
  // Preserve robustness defaults; only the parsed connection fields are set.
  { ..ClientConfig::default(), host, port, password, db }
}

// A single Redis connection.

///|
pub struct Connection {
  mut socket : @socket.Tcp
  config : ClientConfig
  mut recv : Bytes // raw, binary-safe receive buffer
  mut recv_start : Int // cursor: bytes before this have been consumed
  mut closed : Bool // set when the transport is known to be dead/suspect
  mut resp3 : Bool // whether HELLO 3 was negotiated
}

///|
/// Establish a TCP connection, honouring the connect deadline and retrying with
/// exponential backoff on failure.
async fn connect_socket(config : ClientConfig) -> @socket.Tcp raise RedisError {
  let addr = @socket.Addr::parse("\{config.host}:\{config.port}") catch {
    e =>
      raise RedisError::ConnectionFailed(
        "invalid address \{config.host}:\{config.port}: \{e}",
      )
  }
  let mut attempt = 0
  while true {
    let result : @socket.Tcp? = Some(connect_once(config, addr)) catch {
      err => {
        if attempt >= config.max_retries {
          raise err
        }
        None
      }
    }
    match result {
      Some(s) => return s
      None => {
        @async.sleep(backoff_ms(config, attempt)) catch {
          _ => ()
        }
        attempt = attempt + 1
      }
    }
  }
  raise RedisError::ConnectionFailed("connect retries exhausted")
}

///|
/// A single connect attempt: raises Timeout on deadline, ConnectionFailed on
/// transport error.
async fn connect_once(
  config : ClientConfig,
  addr : @socket.Addr,
) -> @socket.Tcp raise RedisError {
  if config.connect_timeout_ms > 0 {
    let r = @async.with_timeout_opt(config.connect_timeout_ms, async fn() {
      @socket.Tcp::connect(addr)
    }) catch {
      e =>
        raise RedisError::ConnectionFailed(
          "connect \{config.host}:\{config.port}: \{e}",
        )
    }
    match r {
      Some(s) => s
      None =>
        raise RedisError::Timeout(
          "connect \{config.host}:\{config.port} timed out after \{config.connect_timeout_ms}ms",
        )
    }
  } else {
    @socket.Tcp::connect(addr) catch {
      e =>
        raise RedisError::ConnectionFailed(
          "connect \{config.host}:\{config.port}: \{e}",
        )
    }
  }
}

///|
/// Exponential backoff delay for connect retry `attempt` (0-based), capped.
fn backoff_ms(config : ClientConfig, attempt : Int) -> Int {
  let mut delay = config.retry_base_delay_ms
  let mut i = 0
  while i < attempt {
    delay = delay * 2
    if delay >= config.retry_max_delay_ms {
      return config.retry_max_delay_ms
    }
    i = i + 1
  }
  if delay > config.retry_max_delay_ms {
    config.retry_max_delay_ms
  } else {
    delay
  }
}

///|
pub async fn Connection::open(
  config : ClientConfig,
) -> Connection raise RedisError {
  let socket = connect_socket(config)
  let conn : Connection = {
    socket,
    config,
    recv: b"",
    recv_start: 0,
    closed: false,
    resp3: false,
  }
  conn.handshake()
  conn
}

///|
/// Run AUTH / SELECT / HELLO 3 on a freshly connected socket.
async fn Connection::handshake(self : Connection) -> Unit raise RedisError {
  match self.config.password {
    Some(pass) =>
      match self.command(["AUTH", pass]) {
        SimpleErr(msg) =>
          raise RedisError::ConnectionFailed("AUTH failed: \{msg}")
        _ => ()
      }
    None => ()
  }
  if self.config.db != 0 {
    match self.command(["SELECT", self.config.db.to_string()]) {
      SimpleErr(msg) =>
        raise RedisError::ConnectionFailed(
          "SELECT \{self.config.db} failed: \{msg}",
        )
      _ => ()
    }
  }
  if self.config.enable_resp3 {
    // Negotiate RESP3; gracefully stay on RESP2 for older servers that reject
    // or do not understand HELLO.
    let ok = try {
      match self.command(["HELLO", "3"]) {
        SimpleErr(_) => false
        _ => true
      }
    } catch {
      _ => false
    }
    self.resp3 = ok
  }
}

///|
/// Reconnect this connection in place (used by the pool to heal a dead conn).
async fn Connection::reopen(self : Connection) -> Unit raise RedisError {
  self.socket.close()
  self.socket = connect_socket(self.config)
  self.recv = b""
  self.recv_start = 0
  self.closed = false
  self.resp3 = false
  self.handshake()
}

///|
pub fn Connection::close(self : Connection) -> Unit {
  self.socket.close()
  self.closed = true
}

///|
async fn Connection::write_frame(
  self : Connection,
  frame : String,
) -> Unit raise RedisError {
  self.socket.write(frame) catch {
    e => {
      self.closed = true
      raise RedisError::IoError("write failed: \{e}")
    }
  }
}

// Send a RESP command and receive one response, bounded by command_timeout_ms.

///|
pub async fn Connection::command(
  self : Connection,
  args : Array[String],
) -> RespValue raise RedisError {
  self.write_frame(encode_command(args))
  let timeout = self.config.command_timeout_ms
  if timeout <= 0 {
    return self.recv_response()
  }
  let r = @async.with_timeout_opt(timeout, async fn() { self.recv_response() }) catch {
    e => raise RedisError::IoError("\{e}")
  }
  match r {
    Some(v) => v
    None => {
      // A partial reply may still be in flight; the connection is unusable.
      self.closed = true
      let name = if args.length() > 0 { args[0] } else { "?" }
      raise RedisError::Timeout("command \{name} timed out after \{timeout}ms")
    }
  }
}

// Send a command without applying the per-command deadline. Use for commands
// that legitimately block server-side (XREADGROUP BLOCK); their own BLOCK bound
// limits how long they wait.

///|
pub async fn Connection::command_blocking(
  self : Connection,
  args : Array[String],
) -> RespValue raise RedisError {
  self.write_frame(encode_command(args))
  self.recv_response()
}

// Send a pipeline (multiple commands) and return all responses in order.

///|
pub async fn Connection::pipeline(
  self : Connection,
  cmds : Array[Array[String]],
) -> Array[RespValue] raise RedisError {
  let mut buf = ""
  for cmd in cmds {
    buf = buf + encode_command(cmd)
  }
  self.write_frame(buf)
  let read_all = async fn() {
    let results : Array[RespValue] = Array::new(capacity=cmds.length())
    for _ in cmds {
      results.push(self.recv_response())
    }
    results
  }
  let timeout = self.config.command_timeout_ms
  if timeout <= 0 {
    return read_all() catch { e => raise RedisError::IoError("\{e}") }
  }
  let r = @async.with_timeout_opt(timeout, read_all) catch {
    e => raise RedisError::IoError("\{e}")
  }
  match r {
    Some(rs) => rs
    None => {
      self.closed = true
      raise RedisError::Timeout("pipeline timed out after \{timeout}ms")
    }
  }
}

// Read bytes from the socket until a complete RESP response is buffered.

///|
async fn Connection::recv_response(
  self : Connection,
) -> RespValue raise RedisError {
  while true {
    if self.recv_start < self.recv.length() {
      let result = decode_resp_bytes(self.recv, self.recv_start) catch {
        e => {
          // A protocol error desynchronises the stream; mark the connection
          // suspect so the pool discards it rather than reusing it.
          self.closed = true
          raise e
        }
      }
      match result {
        Complete(value, next) => {
          self.recv_start = next
          return value
        }
        Incomplete => () // fall through and read more bytes
      }
    }
    let chunk = self.socket.read_some() catch {
      e => {
        self.closed = true
        raise RedisError::IoError("read failed: \{e}")
      }
    }
    match chunk {
      None => {
        self.closed = true
        raise RedisError::IoError("connection closed by server")
      }
      Some(bytes) => {
        // Compact: keep only the unconsumed tail, then append the new chunk.
        let buf = @buffer.from_bytes(self.recv.view(start=self.recv_start))
        buf.write_bytes(bytes.view())
        self.recv = buf.to_bytes()
        self.recv_start = 0
      }
    }
  }
  raise RedisError::IoError("unexpected recv loop exit")
}

// ── Typed command helpers ──────────────────────────────────────────────────

///|
pub async fn Connection::set(
  self : Connection,
  key : String,
  value : String,
) -> Unit raise RedisError {
  let _ = self.command(["SET", key, value])
}

///|
pub async fn Connection::set_px(
  self : Connection,
  key : String,
  value : String,
  px : Int64,
) -> Unit raise RedisError {
  let _ = self.command(["SET", key, value, "PX", px.to_string()])
}

// SET key value NX PX ms — returns true if key was set, false if it existed.

///|
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

///|
pub async fn Connection::get(
  self : Connection,
  key : String,
) -> String? raise RedisError {
  match self.command(["GET", key]) {
    Null => None
    v => Some(v.as_string())
  }
}

///|
pub async fn Connection::del(
  self : Connection,
  key : String,
) -> Int64 raise RedisError {
  self.command(["DEL", key]).as_int()
}

///|
pub async fn Connection::exists(
  self : Connection,
  key : String,
) -> Bool raise RedisError {
  self.command(["EXISTS", key]).as_int() > 0L
}

///|
pub async fn Connection::hset(
  self : Connection,
  key : String,
  fields : Array[String],
) -> Int64 raise RedisError {
  let args : Array[String] = ["HSET", key]
  args.append(fields)
  self.command(args).as_int()
}

///|
pub async fn Connection::hget(
  self : Connection,
  key : String,
  field : String,
) -> String? raise RedisError {
  match self.command(["HGET", key, field]) {
    Null => None
    v => Some(v.as_string())
  }
}

///|
pub async fn Connection::hgetall(
  self : Connection,
  key : String,
) -> Array[String] raise RedisError {
  self
  .command(["HGETALL", key])
  .as_array()
  .map(fn(v) { v.as_string() catch { _ => "" } })
}

///|
pub async fn Connection::zadd(
  self : Connection,
  key : String,
  score : Int64,
  entry : String,
) -> Int64 raise RedisError {
  self.command(["ZADD", key, score.to_string(), entry]).as_int()
}

///|
pub async fn Connection::zrem(
  self : Connection,
  key : String,
  entry : String,
) -> Int64 raise RedisError {
  self.command(["ZREM", key, entry]).as_int()
}

///|
pub async fn Connection::zpopmin(
  self : Connection,
  key : String,
  count : Int,
) -> Array[String] raise RedisError {
  let arr = self.command(["ZPOPMIN", key, count.to_string()]).as_array()
  if arr.is_empty() {
    return []
  }
  // RESP3 wraps each (member, score) pair in a nested array; RESP2 is flat.
  match arr[0] {
    Array(_) | Push(_) => {
      let result : Array[String] = Array::new(capacity=arr.length() * 2)
      for item in arr {
        for v in item.as_array() {
          result.push(v.as_string() catch { _ => "" })
        }
      }
      result
    }
    _ => arr.map(fn(v) { v.as_string() catch { _ => "" } })
  }
}

///|
pub async fn Connection::zrangebyscore(
  self : Connection,
  key : String,
  min : String,
  max : String,
) -> Array[String] raise RedisError {
  self
  .command(["ZRANGEBYSCORE", key, min, max])
  .as_array()
  .map(fn(v) { v.as_string() catch { _ => "" } })
}

///|
pub async fn Connection::xadd(
  self : Connection,
  key : String,
  fields : Array[String],
) -> String raise RedisError {
  let args : Array[String] = ["XADD", key, "*"]
  args.append(fields)
  self.command(args).as_string()
}

///|
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

///|
pub struct XReadGroupEntry {
  stream_id : String
  fields : Array[String]
} derive(Debug)

///|
pub async fn Connection::xreadgroup(
  self : Connection,
  group : String,
  consumer : String,
  stream_key : String,
  count~ : Int,
  block_ms~ : Int,
) -> Array[XReadGroupEntry] raise RedisError {
  // XREADGROUP BLOCK blocks server-side for up to block_ms, which may exceed
  // command_timeout_ms — so it goes through the deadline-free path.
  let resp = self.command_blocking([
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

///|
pub async fn Connection::xack(
  self : Connection,
  key : String,
  group : String,
  stream_id : String,
) -> Int64 raise RedisError {
  self.command(["XACK", key, group, stream_id]).as_int()
}

///|
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

///|
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

///|
pub async fn Connection::script_load(
  self : Connection,
  script : String,
) -> String raise RedisError {
  self.command(["SCRIPT", "LOAD", script]).as_string()
}

///|
pub async fn Connection::publish(
  self : Connection,
  channel : String,
  message : String,
) -> Int64 raise RedisError {
  self.command(["PUBLISH", channel, message]).as_int()
}

///|
/// Put the connection into subscribe mode for the given channels.
/// After this call the connection can only be used with `read_pub_message`;
/// ordinary commands will not work until the connection is closed.
pub async fn Connection::subscribe(
  self : Connection,
  channels : Array[String],
) -> Unit raise RedisError {
  let args : Array[String] = ["SUBSCRIBE"]
  args.append(channels)
  self.write_frame(encode_command(args))
  // Consume one subscription confirmation per channel.
  for _ in channels {
    let _ = self.recv_response()
  }
}

///|
/// Read the next pub/sub message from a subscribed connection.
/// Skips subscription-confirmation frames and returns only payload messages
/// as (channel, data).  Blocks until a message arrives or an error occurs.
pub async fn Connection::read_pub_message(
  self : Connection,
) -> (String, String) raise RedisError {
  while true {
    let resp = self.recv_response()
    match extract_pub_message(resp) {
      Some(pair) => return pair
      None => () // skip subscription confirmations, pongs, etc.
    }
  }
  raise RedisError::IoError("read_pub_message loop exited unexpectedly")
}

// Parse a pub/sub push notification (RESP2 array or RESP3 push frame).
// Returns Some((channel, data)) for "message" type; None otherwise.

///|
fn extract_pub_message(resp : RespValue) -> (String, String)? {
  match resp {
    Array(parts) | Push(parts) => {
      if parts.length() < 3 {
        return None
      }
      let msg_type = match parts[0] {
        BulkStr(s) | SimpleStr(s) => s
        _ => return None
      }
      if msg_type != "message" {
        return None
      }
      let channel = match parts[1] {
        BulkStr(s) | SimpleStr(s) => s
        _ => ""
      }
      let data = match parts[2] {
        BulkStr(s) | SimpleStr(s) => s
        _ => ""
      }
      Some((channel, data))
    }
    _ => None
  }
}

///|
/// Iterate over all keys matching `match_pattern` using Redis cursor-based
/// SCAN. Returns (next_cursor, keys_batch). When next_cursor == "0" a full
/// iteration is complete.
pub async fn Connection::scan(
  self : Connection,
  cursor : String,
  match_pattern? : String = "*",
  count? : Int = 100,
) -> (String, Array[String]) raise RedisError {
  let resp = self.command([
    "SCAN",
    cursor,
    "MATCH",
    match_pattern,
    "COUNT",
    count.to_string(),
  ])
  match resp {
    Array(parts) | Set(parts) if parts.length() == 2 => {
      let next_cursor = parts[0].as_string()
      let keys = parts[1]
        .as_array()
        .map(fn(v) { v.as_string() catch { _ => "" } })
      (next_cursor, keys)
    }
    _ => raise RedisError::ParseError("unexpected SCAN response")
  }
}

// Parse XREADGROUP response: [[stream_key, [[id, [f, v, ...]], ...]]]

///|
fn parse_xreadgroup_response(
  resp : RespValue,
) -> Array[XReadGroupEntry] raise RedisError {
  match resp {
    Null => []
    Array(streams) => {
      // RESP2: [[stream_key, [[id, fields], ...]]]
      if streams.is_empty() {
        return []
      }
      let stream = streams[0].as_array()
      if stream.length() < 2 {
        return []
      }
      parse_stream_entries(stream[1].as_array())
    }
    Map(pairs) | Push(pairs) => {
      // RESP3: { stream_key: [[id, fields], ...] } flattened to [key, entries].
      if pairs.length() < 2 {
        return []
      }
      parse_stream_entries(pairs[1].as_array())
    }
    _ => raise RedisError::ParseError("unexpected XREADGROUP response")
  }
}

///|
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
    let fields = pair[1]
      .as_array()
      .map(fn(v) { v.as_string() catch { _ => "" } })
    result.push({ stream_id, fields })
  }
  result
}

// ── URL parsing helpers ────────────────────────────────────────────────────

///|
fn split_once(s : String, sep : String) -> (String, String) {
  match s.find(sep) {
    Some(idx) => (s[:idx].to_owned(), s[idx + sep.length():].to_owned())
    None => (s, "")
  }
}

///|
fn split_once_rev(s : String, sep : String) -> (String, String) {
  match s.rev_find(sep) {
    Some(idx) => (s[:idx].to_owned(), s[idx + sep.length():].to_owned())
    None => ("", s)
  }
}
