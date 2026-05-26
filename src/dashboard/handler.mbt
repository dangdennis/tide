// HTTP handler for the tide dashboard.
//
// Exposes:
//   GET  /                          → HTML page
//   GET  /api/queues                → JSON array of QueueStats
//   GET  /api/jobs?queue=Q&state=S  → JSON array of jobs
//   GET  /api/jobs/:id              → JSON job object
//   POST /api/jobs/:id/retry        → retry a job
//   POST /api/jobs/:id/cancel       → cancel a job
//   DELETE /api/jobs/:id            → delete a terminal job

// ── Entry point ───────────────────────────────────────────────────────────────

///|
/// Start the HTTP server.  Blocks until an error occurs.
pub async fn serve(
  engine : @engine.RedisEngine,
  queues : Map[String, Int],
  port   : Int,
) -> Unit raise Error {
  let addr = @socket.Addr::parse("0.0.0.0:\{port}")
  let server = @http.Server(addr)
  let pool = engine.pool
  server.run_forever() <| ((request, _body, conn) => {
    route(engine, pool, queues, request, conn) catch {
      e => {
        conn.send_response(500, "Internal Server Error",
          extra_headers={"Content-Type": "text/plain"})
        conn.write_string("\{e}")
        conn.end_response()
      }
    }
  })
}

// ── Router ────────────────────────────────────────────────────────────────────

async fn route(
  engine : @engine.RedisEngine,
  pool   : @redis.Pool,
  queues : Map[String, Int],
  req    : @http.Request,
  conn   : @http.ServerConnection,
) -> Unit raise Error {
  // Split path and query string.
  let (path, query) = split_path_query(req.path)
  match (req.meth, path) {
    (Get, "/") => send_html(conn, DASHBOARD_HTML)
    (Get, "/api/queues") => handle_queues(pool, queues, conn)
    (Get, "/api/jobs") => handle_list_jobs(pool, query, conn)
    (Get, p) if p.starts_with("/api/jobs/") => {
      let rest = p["/api/jobs/".length():].to_owned()
      if rest.ends_with("/retry") {
        // GET /api/jobs/:id/retry is not used; ignore
        send_json(conn, "null")
      } else {
        handle_get_job(pool, rest, conn)
      }
    }
    (Post, p) if p.starts_with("/api/jobs/") => {
      let rest = p["/api/jobs/".length():].to_owned()
      if rest.ends_with("/retry") {
        let id = rest[0:rest.length() - "/retry".length()].to_owned()
        retry_job(engine, id)
        send_json(conn, "{\"ok\":true}")
      } else if rest.ends_with("/cancel") {
        let id = rest[0:rest.length() - "/cancel".length()].to_owned()
        cancel_job(engine, id)
        send_json(conn, "{\"ok\":true}")
      } else {
        send_json(conn, "{\"ok\":false}")
      }
    }
    (Delete, p) if p.starts_with("/api/jobs/") => {
      let id = p["/api/jobs/".length():].to_owned()
      delete_job(pool, id)
      send_json(conn, "{\"ok\":true}")
    }
    _ => {
      conn.send_response(404, "Not Found",
        extra_headers={"Content-Type": "text/plain"})
      conn.write_string("404")
      conn.end_response()
    }
  }
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn handle_queues(
  pool   : @redis.Pool,
  queues : Map[String, Int],
  conn   : @http.ServerConnection,
) -> Unit raise Error {
  let parts : Array[String] = []
  for queue, _ in queues {
    let stats = queue_stats(pool, queue)
    parts.push(queue_stats_to_json(stats))
  }
  send_json(conn, "[" + parts.join(",") + "]")
}

async fn handle_list_jobs(
  pool  : @redis.Pool,
  query : String,
  conn  : @http.ServerConnection,
) -> Unit raise Error {
  let params = parse_query(query)
  let queue  = params.get("queue").unwrap_or("default")
  let state  = params.get("state").unwrap_or("available")
  let limit  = @string.parse_int(params.get("limit").unwrap_or("50"), base=10) catch {
    _ => 50
  }
  let offset = @string.parse_int(params.get("offset").unwrap_or("0"), base=10) catch {
    _ => 0
  }
  let jobs = list_jobs(pool, queue, state, limit, offset)
  let parts = jobs.map(fn(j) { job_to_json(j) })
  send_json(conn, "[" + parts.join(",") + "]")
}

async fn handle_get_job(
  pool : @redis.Pool,
  id   : String,
  conn : @http.ServerConnection,
) -> Unit raise Error {
  match get_job(pool, id) {
    None => {
      conn.send_response(404, "Not Found",
        extra_headers={"Content-Type": "application/json"})
      conn.write_string("{\"error\":\"not found\"}")
      conn.end_response()
    }
    Some(j) => send_json(conn, job_to_json(j))
  }
}

// ── Response helpers ──────────────────────────────────────────────────────────

async fn send_html(
  conn : @http.ServerConnection,
  body : String,
) -> Unit raise Error {
  conn.send_response(200, "OK", extra_headers={
    "Content-Type": "text/html; charset=utf-8",
  })
  conn.write_string(body)
  conn.end_response()
}

async fn send_json(
  conn : @http.ServerConnection,
  body : String,
) -> Unit raise Error {
  conn.send_response(200, "OK", extra_headers={
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
  })
  conn.write_string(body)
  conn.end_response()
}

// ── JSON serialization ────────────────────────────────────────────────────────

fn queue_stats_to_json(s : QueueStats) -> String {
  "{\"queue\":\{json_str(s.queue)},\"available\":\{s.available},\"scheduled\":\{s.scheduled},\"executing\":\{s.executing}}"
}

fn job_to_json(j : @job.Job) -> String {
  let buf = StringBuilder::new()
  buf.write_string("{")
  buf.write_string("\"id\":\{json_str(j.id)},")
  buf.write_string("\"queue\":\{json_str(j.queue)},")
  buf.write_string("\"worker\":\{json_str(j.worker)},")
  buf.write_string("\"args\":\{json_str(j.args)},")
  buf.write_string("\"state\":\{json_str(j.state.to_string())},")
  buf.write_string("\"attempt\":\{j.attempt},")
  buf.write_string("\"max_attempts\":\{j.max_attempts},")
  buf.write_string("\"priority\":\{j.priority},")
  buf.write_string("\"inserted_at\":\{j.inserted_at},")
  buf.write_string("\"scheduled_at\":\{j.scheduled_at},")
  buf.write_string("\"attempted_at\":\{json_int64_opt(j.attempted_at)},")
  buf.write_string("\"completed_at\":\{json_int64_opt(j.completed_at)},")
  buf.write_string("\"discarded_at\":\{json_int64_opt(j.discarded_at)},")
  buf.write_string("\"cancelled_at\":\{json_int64_opt(j.cancelled_at)},")
  buf.write_string("\"tags\":\{json_str_array(j.tags)},")
  buf.write_string("\"errors\":\{json_str_array(j.errors)},")
  buf.write_string("\"attempted_by\":\{json_str_array(j.attempted_by)},")
  buf.write_string("\"meta\":\{json_str(j.meta)}")
  buf.write_string("}")
  buf.to_string()
}

fn json_str(s : String) -> String {
  "\"" + s
    .replace_all(old="\\", new="\\\\")
    .replace_all(old="\"", new="\\\"")
    .replace_all(old="\n", new="\\n")
    .replace_all(old="\r", new="\\r")
    .replace_all(old="\t", new="\\t") + "\""
}

fn json_str_array(arr : Array[String]) -> String {
  "[" + arr.map(fn(s) { json_str(s) }).join(",") + "]"
}

fn json_int64_opt(v : Int64?) -> String {
  match v {
    None => "null"
    Some(n) => n.to_string()
  }
}

// ── URL parsing ───────────────────────────────────────────────────────────────

/// Split "/path?key=val" into ("/path", "key=val").
fn split_path_query(raw : String) -> (String, String) {
  let mut qi = -1
  let mut i = 0
  while i < raw.length() {
    if raw[i] == '?' {
      qi = i
      break
    }
    i = i + 1
  }
  if qi < 0 {
    (raw, "")
  } else {
    (raw[0:qi].to_owned(), raw[qi + 1 :].to_owned())
  }
}

/// Parse "key=val&key2=val2" into a Map.
fn parse_query(query : String) -> Map[String, String] {
  let map : Map[String, String] = Map([])
  if query.is_empty() {
    return map
  }
  for part in query.split("&") {
    let part_str = part.to_owned()
    // Find '=' index inline.
    let mut eq = -1
    let mut pi = 0
    while pi < part_str.length() {
      if part_str[pi] == '=' {
        eq = pi
        break
      }
      pi = pi + 1
    }
    if eq >= 0 {
      map[part_str[0:eq].to_owned()] = part_str[eq + 1:].to_owned()
    } else if !part_str.is_empty() {
      map[part_str] = ""
    }
  }
  map
}
