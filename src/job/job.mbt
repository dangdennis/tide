// Job state machine:
//   scheduled → available → executing → completed
//                                ↓
//                            retryable → available
//                                ↓
//                            discarded
//                                ↓
//                            cancelled

///|
pub(all) enum JobState {
  Available
  Scheduled
  Executing
  Completed
  Retryable
  Discarded
  Cancelled
} derive(Debug, Eq)

///|
pub impl Show for JobState with fn output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub fn JobState::to_string(self : JobState) -> String {
  match self {
    Available => "available"
    Scheduled => "scheduled"
    Executing => "executing"
    Completed => "completed"
    Retryable => "retryable"
    Discarded => "discarded"
    Cancelled => "cancelled"
  }
}

///|
pub fn JobState::from_string(s : String) -> JobState? {
  match s {
    "available" => Some(Available)
    "scheduled" => Some(Scheduled)
    "executing" => Some(Executing)
    "completed" => Some(Completed)
    "retryable" => Some(Retryable)
    "discarded" => Some(Discarded)
    "cancelled" => Some(Cancelled)
    _ => None
  }
}

///|
pub(all) struct Job {
  id : String
  queue : String
  worker : String
  args : String // JSON-encoded arbitrary payload
  meta : String // JSON-encoded plugin metadata (cron: true, etc.)
  errors : Array[String] // JSON-encoded error entries per failed attempt
  tags : Array[String]
  state : JobState
  attempt : Int
  max_attempts : Int
  priority : Int // 0–9; lower number = higher priority
  inserted_at : Int64 // unix milliseconds
  scheduled_at : Int64 // unix milliseconds; when the job may run
  attempted_at : Int64?
  completed_at : Int64?
  discarded_at : Int64?
  cancelled_at : Int64?
  attempted_by : Array[String] // node IDs that attempted this job
} derive(Debug)

// A job claimed from the Redis Stream. stream_id is needed for XACK.

///|
pub(all) struct FetchedJob {
  job : Job
  stream_id : String
} derive(Debug)

///|
pub fn FetchedJob::new(job : Job, stream_id : String) -> FetchedJob {
  { job, stream_id }
}

///|
pub fn Job::new(queue : String, worker : String) -> Job {
  {
    id: "",
    queue,
    worker,
    args: "{}",
    meta: "{}",
    errors: [],
    tags: [],
    state: Available,
    attempt: 0,
    max_attempts: 3,
    priority: 5,
    inserted_at: 0L,
    scheduled_at: 0L,
    attempted_at: None,
    completed_at: None,
    discarded_at: None,
    cancelled_at: None,
    attempted_by: [],
  }
}

///|
pub fn Job::with_args(self : Job, args : String) -> Job {
  { ..self, args, }
}

///|
pub fn Job::with_meta(self : Job, meta : String) -> Job {
  { ..self, meta, }
}

///|
pub fn Job::with_tags(self : Job, tags : Array[String]) -> Job {
  { ..self, tags, }
}

///|
pub fn Job::with_max_attempts(self : Job, max_attempts : Int) -> Job {
  { ..self, max_attempts, }
}

// 0 = highest priority, 9 = lowest

///|
pub fn Job::with_priority(self : Job, priority : Int) -> Job {
  { ..self, priority, }
}

// Unix milliseconds when the job should first become runnable.

///|
pub fn Job::with_scheduled_at(self : Job, scheduled_at : Int64) -> Job {
  { ..self, scheduled_at, }
}

// Convert job to Redis HSET field-value pairs.
// Returns alternating [field, value, field, value, ...].

///|
pub fn Job::to_hash_fields(self : Job) -> Array[String] {
  let fields : Array[String] = []
  fields.push("id")
  fields.push(self.id)
  fields.push("queue")
  fields.push(self.queue)
  fields.push("worker")
  fields.push(self.worker)
  fields.push("args")
  fields.push(self.args)
  fields.push("meta")
  fields.push(self.meta)
  fields.push("errors")
  fields.push(json_string_array(self.errors))
  fields.push("tags")
  fields.push(json_string_array(self.tags))
  fields.push("state")
  fields.push(self.state.to_string())
  fields.push("attempt")
  fields.push(self.attempt.to_string())
  fields.push("max_attempts")
  fields.push(self.max_attempts.to_string())
  fields.push("priority")
  fields.push(self.priority.to_string())
  fields.push("inserted_at")
  fields.push(self.inserted_at.to_string())
  fields.push("scheduled_at")
  fields.push(self.scheduled_at.to_string())
  fields.push("attempted_at")
  fields.push(option_int64_to_string(self.attempted_at))
  fields.push("completed_at")
  fields.push(option_int64_to_string(self.completed_at))
  fields.push("discarded_at")
  fields.push(option_int64_to_string(self.discarded_at))
  fields.push("cancelled_at")
  fields.push(option_int64_to_string(self.cancelled_at))
  fields.push("attempted_by")
  fields.push(json_string_array(self.attempted_by))
  fields
}

// Reconstruct a Job from Redis HGETALL result (flat list of field, value pairs).

///|
pub fn Job::from_hash_fields(pairs : Array[String]) -> Job? {
  let map : Map[String, String] = Map([])
  let mut i = 0
  while i + 1 < pairs.length() {
    map[pairs[i]] = pairs[i + 1]
    i = i + 2
  }
  let id = match map.get("id") {
    Some(v) => v
    None => return None
  }
  let queue = match map.get("queue") {
    Some(v) => v
    None => return None
  }
  let worker = match map.get("worker") {
    Some(v) => v
    None => return None
  }
  let state = match map.get("state") {
    Some(s) =>
      match JobState::from_string(s) {
        Some(st) => st
        None => return None
      }
    None => return None
  }
  let attempt = match map.get("attempt").bind(fn(s) { parse_int(s) }) {
    Some(v) => v
    None => return None
  }
  let max_attempts = match
    map.get("max_attempts").bind(fn(s) { parse_int(s) }) {
    Some(v) => v
    None => return None
  }
  let priority = match map.get("priority").bind(fn(s) { parse_int(s) }) {
    Some(v) => v
    None => return None
  }
  let inserted_at = match
    map.get("inserted_at").bind(fn(s) { parse_int64(s) }) {
    Some(v) => v
    None => return None
  }
  let scheduled_at = match
    map.get("scheduled_at").bind(fn(s) { parse_int64(s) }) {
    Some(v) => v
    None => return None
  }
  Some({
    id,
    queue,
    worker,
    args: map.get("args").unwrap_or("{}"),
    meta: map.get("meta").unwrap_or("{}"),
    errors: parse_json_string_array(map.get("errors").unwrap_or("[]")),
    tags: parse_json_string_array(map.get("tags").unwrap_or("[]")),
    state,
    attempt,
    max_attempts,
    priority,
    inserted_at,
    scheduled_at,
    attempted_at: map.get("attempted_at").bind(fn(s) { parse_int64_opt(s) }),
    completed_at: map.get("completed_at").bind(fn(s) { parse_int64_opt(s) }),
    discarded_at: map.get("discarded_at").bind(fn(s) { parse_int64_opt(s) }),
    cancelled_at: map.get("cancelled_at").bind(fn(s) { parse_int64_opt(s) }),
    attempted_by: parse_json_string_array(
      map.get("attempted_by").unwrap_or("[]"),
    ),
  })
}

// Compute the sorted-set score for this job.
// Lower priority integer = higher precedence; earlier scheduled_at = higher precedence.

///|
pub fn Job::sorted_set_score(self : Job) -> Int64 {
  @score.compute(Int64::from_int(self.priority), self.scheduled_at)
}

// Default backoff: min(attempt^4 + 15, 86400) seconds, then convert to ms.
// Guard: attempt^4 overflows 32-bit Int for |attempt| >= 216; beyond that
// the 86400 s cap already applies, so we short-circuit to avoid UB.

///|
pub fn default_backoff_ms(attempt : Int) -> Int64 {
  let secs = if attempt >= 216 || attempt <= -216 {
    86400
  } else {
    let n = attempt * attempt * attempt * attempt + 15
    if n < 86400 {
      n
    } else {
      86400
    }
  }
  proof_assert secs >= 15 // lower bound: attempt 0 → 15 s
  proof_assert secs <= 86400 // upper bound: capped at 24 h
  Int64::from_int(secs) * 1000L
}

///|
pub fn json_string_array(arr : Array[String]) -> String {
  let items = arr.map(fn(s) { "\"" + escape_json_string(s) + "\"" })
  "[" + items.join(",") + "]"
}

///|
fn parse_json_string_array(s : String) -> Array[String] {
  let result : Array[String] = []
  let len = s.length()
  let mut i = 0
  while i < len && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n') {
    i = i + 1
  }
  if i >= len || s[i] != '[' {
    return result
  }
  i = i + 1
  while i < len {
    while i < len && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n') {
      i = i + 1
    }
    if i >= len || s[i] == ']' {
      break
    }
    if s[i] == ',' {
      i = i + 1
      continue
    }
    if s[i] != '"' {
      break
    }
    i = i + 1
    let buf = StringBuilder::new()
    while i < len && s[i] != '"' {
      if s[i] == '\\' && i + 1 < len {
        match s[i + 1] {
          '"' => buf.write_char('"')
          '\\' => buf.write_char('\\')
          'n' => buf.write_char('\n')
          'r' => buf.write_char('\r')
          't' => buf.write_char('\t')
          _ => buf.write_string(s[i + 1:i + 2].to_owned())
        }
        i = i + 2
      } else {
        buf.write_string(s[i:i + 1].to_owned())
        i = i + 1
      }
    }
    if i < len {
      i = i + 1
    }
    result.push(buf.to_string())
  }
  result
}

///|
fn escape_json_string(s : String) -> String {
  s
  .replace_all(old="\\", new="\\\\")
  .replace_all(old="\"", new="\\\"")
  .replace_all(old="\n", new="\\n")
  .replace_all(old="\r", new="\\r")
  .replace_all(old="\t", new="\\t")
}

///|
fn option_int64_to_string(v : Int64?) -> String {
  match v {
    Some(n) => n.to_string()
    None => ""
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
fn parse_int64_opt(s : String) -> Int64? {
  if s.is_empty() {
    None
  } else {
    parse_int64(s)
  }
}
