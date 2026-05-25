// Cron expression parser.
//
// Supported formats:
//   @hourly             → "0 * * * *"
//   @daily / @midnight  → "0 0 * * *"
//   @weekly             → "0 0 * * 0"
//   @monthly            → "0 0 1 * *"
//   @yearly / @annually → "0 0 1 1 *"
//   Standard 5-field:   "minute hour dom month dow"
//
// Per-field syntax (comma-separated list of):
//   *          — any value
//   */n        — every n-th value (step over full range)
//   v          — exact value
//   a-b        — inclusive range
//   a-b/n      — range with step

///|
pub(all) struct CronExpr {
  minutes : Array[Bool] // [0..59]
  hours : Array[Bool] // [0..23]
  doms : Array[Bool] // size 32; index = day (1-31)
  months : Array[Bool] // size 13; index = month (1-12)
  dows : Array[Bool] // [0..6]; 0=Sun, 6=Sat
}

///|
pub suberror CronError {
  InvalidExpression(String)
}

///|
pub impl Show for CronError with fn output(self, logger) {
  match self {
    InvalidExpression(msg) => logger.write_string("CronError(\{msg})")
  }
}

///|
pub fn CronExpr::parse(s : String) -> CronExpr raise CronError {
  let normalized = match s {
    "@hourly" => "0 * * * *"
    "@daily" | "@midnight" => "0 0 * * *"
    "@weekly" => "0 0 * * 0"
    "@monthly" => "0 0 1 * *"
    "@yearly" | "@annually" => "0 0 1 1 *"
    other => other
  }
  let fields = split_fields(normalized)
  if fields.length() != 5 {
    raise CronError::InvalidExpression(
      "expected 5 fields, got \{fields.length()}: \{normalized}",
    )
  }
  {
    minutes: parse_field(fields[0], 0, 59),
    hours: parse_field(fields[1], 0, 23),
    doms: parse_field(fields[2], 1, 31),
    months: parse_field(fields[3], 1, 12),
    dows: parse_field(fields[4], 0, 6),
  }
}

///|
/// Returns true if this expression matches the given Unix-millisecond timestamp.
/// The check is minute-granular — seconds/sub-seconds are ignored.
pub fn CronExpr::matches_ms(self : CronExpr, ts_ms : Int64) -> Bool {
  let dt = ms_to_datetime(ts_ms)
  self.minutes[dt.minute] &&
  self.hours[dt.hour] &&
  self.doms[dt.day] &&
  self.months[dt.month] &&
  self.dows[dt.weekday]
}

// ── Internal calendar types ───────────────────────────────────────────────────

///|
priv struct DateTime {
  minute : Int // 0–59
  hour : Int // 0–23
  day : Int // 1–31
  month : Int // 1–12
  weekday : Int // 0=Sun, 6=Sat
}

// Convert a Unix millisecond timestamp to calendar components (UTC).
// Uses Howard Hinnant's civil_from_days algorithm for Gregorian date.

///|
fn ms_to_datetime(ts_ms : Int64) -> DateTime {
  let ts_sec = ts_ms / 1000L
  let day_sec = (ts_sec % 86400L).to_int()
  let hour = day_sec / 3600
  let minute = day_sec % 3600 / 60
  let days = (ts_sec / 86400L).to_int()
  // Epoch (1970-01-01) was a Thursday → 4 in 0=Sun convention.
  let weekday = (days % 7 + 4 + 7) % 7
  // Hinnant civil_from_days:
  let z = days + 719468
  let era = (if z >= 0 { z } else { z - 146096 }) / 146097
  let doe = z - era * 146097
  let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365
  let doy = doe - (365 * yoe + yoe / 4 - yoe / 100)
  let mp = (5 * doy + 2) / 153
  let d = doy - (153 * mp + 2) / 5 + 1
  let m = if mp < 10 { mp + 3 } else { mp - 9 }
  { minute, hour, day: d, month: m, weekday }
}

// ── Field parser ──────────────────────────────────────────────────────────────

// Split a cron expression into its 5 whitespace-delimited fields.

///|
fn split_fields(s : String) -> Array[String] {
  let fields : Array[String] = []
  let len = s.length()
  let mut i = 0
  while i < len {
    while i < len && s[i] == ' ' {
      i = i + 1
    }
    if i >= len {
      break
    }
    let start = i
    while i < len && s[i] != ' ' {
      i = i + 1
    }
    if i > start {
      fields.push(s[start:i].to_owned())
    }
  }
  fields
}

// Parse a single cron field (e.g. "0,15,30,45" or "*/5" or "1-5/2")
// into a Boolean lookup array indexed by value.

///|
fn parse_field(
  s : String,
  min_val : Int,
  max_val : Int,
) -> Array[Bool] raise CronError {
  let bits = Array::make(max_val + 1, false)
  let len = s.length()
  let mut start = 0
  while start <= len {
    let mut end = start
    while end < len && s[end] != ',' {
      end = end + 1
    }
    if end > start {
      parse_spec(s[start:end].to_owned(), min_val, max_val, bits)
    }
    if end >= len {
      break
    }
    start = end + 1
  }
  bits
}

// Parse one spec token and mark matching positions in `bits`.

///|
fn parse_spec(
  part : String,
  min_val : Int,
  max_val : Int,
  bits : Array[Bool],
) -> Unit raise CronError {
  let len = part.length()
  if len == 0 {
    raise CronError::InvalidExpression("empty field spec")
  }
  // Handle wildcard forms: "*" and "*/n"
  if part[0] == '*' {
    if len == 1 {
      let mut v = min_val
      while v <= max_val {
        bits[v] = true
        v = v + 1
      }
    } else if len >= 3 && part[1] == '/' {
      let step = parse_single_int(part, 2)
      if step <= 0 {
        raise CronError::InvalidExpression("step must be > 0: \{part}")
      }
      let mut v = min_val
      while v <= max_val {
        bits[v] = true
        v = v + step
      }
    } else {
      raise CronError::InvalidExpression("invalid *-spec: \{part}")
    }
    return
  }
  // Number or range
  match parse_int_at(part, 0) {
    None => raise CronError::InvalidExpression("expected number: \{part}")
    Some((a, pos1)) => {
      if a < min_val || a > max_val {
        raise CronError::InvalidExpression(
          "value \{a} out of range [\{min_val}..\{max_val}]: \{part}",
        )
      }
      if pos1 >= len {
        // Exact single value.
        bits[a] = true
        return
      }
      if part[pos1] != '-' {
        raise CronError::InvalidExpression(
          "unexpected char at pos \{pos1}: \{part}",
        )
      }
      // Range: a-b or a-b/n
      match parse_int_at(part, pos1 + 1) {
        None =>
          raise CronError::InvalidExpression("expected number after -: \{part}")
        Some((b, pos2)) => {
          if b < min_val || b > max_val || b < a {
            raise CronError::InvalidExpression("invalid range: \{part}")
          }
          let step = if pos2 < len && part[pos2] == '/' {
            let n = parse_single_int(part, pos2 + 1)
            if n <= 0 {
              raise CronError::InvalidExpression("step must be > 0: \{part}")
            }
            n
          } else {
            1
          }
          let mut v = a
          while v <= b {
            bits[v] = true
            v = v + step
          }
        }
      }
    }
  }
}

// Parse an integer starting at position `start`; returns (value, next_pos).
// s[i] returns UInt16 on the native backend, so use .to_int() for range checks.

///|
fn parse_int_at(s : String, start : Int) -> (Int, Int)? {
  let len = s.length()
  if start >= len {
    return None
  }
  let first = s[start].to_int()
  if first < 48 || first > 57 { // '0'=48, '9'=57
    return None
  }
  let mut n = 0
  let mut i = start
  while i < len &&
        ({
          let d = s[i].to_int()
          d >= 48 && d <= 57
        }) {
    n = n * 10 + (s[i].to_int() - 48)
    i = i + 1
  }
  Some((n, i))
}

///|
fn parse_single_int(s : String, start : Int) -> Int raise CronError {
  match parse_int_at(s, start) {
    None =>
      raise CronError::InvalidExpression("expected integer at \{start}: \{s}")
    Some((n, _)) => n
  }
}
