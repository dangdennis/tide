// Helpers for the flat JSON object stored in Job::meta.
// Only string and boolean top-level values are supported.
// For richer structures, use Job::with_meta directly.

///|
/// Get a string value from the meta JSON object.  Returns None when the key
/// is absent or the value is not a JSON string.
pub fn meta_get_string(meta : String, key : String) -> String? {
  let needle = "\"" + key + "\":"
  match meta.find(needle) {
    None => None
    Some(idx) => {
      let after = idx + needle.length()
      let len = meta.length()
      if after >= len || meta[after] != '"' {
        return None
      }
      let mut i = after + 1
      let buf = StringBuilder::new()
      while i < len && meta[i] != '"' {
        if meta[i] == '\\' && i + 1 < len {
          match meta[i + 1] {
            '"' => buf.write_char('"')
            '\\' => buf.write_char('\\')
            'n' => buf.write_char('\n')
            'r' => buf.write_char('\r')
            _ => buf.write_string(meta[i + 1:i + 2].to_owned())
          }
          i = i + 2
        } else {
          buf.write_string(meta[i:i + 1].to_owned())
          i = i + 1
        }
      }
      Some(buf.to_string())
    }
  }
}

///|
/// Get a boolean value from the meta JSON object.  Returns None when the key
/// is absent or the value is not a JSON boolean.
pub fn meta_get_bool(meta : String, key : String) -> Bool? {
  let needle = "\"" + key + "\":"
  match meta.find(needle) {
    None => None
    Some(idx) => {
      let after = idx + needle.length()
      let len = meta.length()
      if after + 4 <= len && meta[after:after + 4].to_owned() == "true" {
        Some(true)
      } else if after + 5 <= len && meta[after:after + 5].to_owned() == "false" {
        Some(false)
      } else {
        None
      }
    }
  }
}

///|
/// Return a new meta JSON string with `key` set to the given string value.
/// Replaces the existing value if `key` is already present.
pub fn meta_set_string(meta : String, key : String, value : String) -> String {
  let escaped = escape_meta_string(value)
  let kv = "\"" + key + "\":\"" + escaped + "\""
  meta_upsert(meta, key, kv)
}

///|
/// Return a new meta JSON string with `key` set to the given boolean value.
/// Replaces the existing value if `key` is already present.
pub fn meta_set_bool(meta : String, key : String, value : Bool) -> String {
  let raw_val = if value { "true" } else { "false" }
  let kv = "\"" + key + "\":" + raw_val
  meta_upsert(meta, key, kv)
}

// Replace or insert a raw "key":rawval pair in a compact flat JSON object.

///|
fn meta_upsert(meta : String, key : String, kv : String) -> String {
  let needle = "\"" + key + "\":"
  match meta.find(needle) {
    Some(start) => {
      let val_start = start + needle.length()
      let val_end = skip_json_value(meta, val_start)
      meta[:start].to_owned() + kv + meta[val_end:].to_owned()
    }
    None =>
      if meta == "{}" {
        "{" + kv + "}"
      } else {
        match meta.rev_find("}") {
          None => "{" + kv + "}"
          Some(close) => meta[:close].to_owned() + "," + kv + "}"
        }
      }
  }
}

// Advance past one JSON value starting at `start`.
// Handles quoted strings (with escapes) and unquoted literals (true/false/null/numbers).

///|
fn skip_json_value(s : String, start : Int) -> Int {
  let len = s.length()
  if start >= len {
    return start
  }
  if s[start] == '"' {
    let mut i = start + 1
    while i < len {
      if s[i] == '\\' && i + 1 < len {
        i = i + 2
      } else if s[i] == '"' {
        return i + 1
      } else {
        i = i + 1
      }
    }
    i
  } else {
    let mut i = start
    while i < len && s[i] != ',' && s[i] != '}' && s[i] != ' ' && s[i] != '\n' {
      i = i + 1
    }
    i
  }
}

///|
fn escape_meta_string(s : String) -> String {
  s
  .replace_all(old="\\", new="\\\\")
  .replace_all(old="\"", new="\\\"")
  .replace_all(old="\n", new="\\n")
  .replace_all(old="\r", new="\\r")
}
