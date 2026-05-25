// Unique job support: prevent duplicate insertions within a time window.
//
// UniqueConfig describes how uniqueness is determined:
//   - period_ms:  how long the uniqueness lock persists (Redis key TTL)
//   - fields:     which job fields feed into the key
//                 supported values: "queue", "worker", "args", "tags"
//
// Usage:
//   let config = UniqueConfig::new(3_600_000L) // 1-hour lock
//   let result = instance.insert_unique(job, config)
//
// The actual atomic Redis insertion is handled by RedisEngine::insert_unique_job
// via the unique_insert.lua Lua script.

///|
pub(all) struct UniqueConfig {
  period_ms : Int64
  fields : Array[String]
}

///|
/// Create a UniqueConfig with the default field set (queue + worker + args).
pub fn UniqueConfig::new(period_ms : Int64) -> UniqueConfig {
  { period_ms, fields: ["queue", "worker", "args"] }
}

///|
pub fn UniqueConfig::with_fields(
  self : UniqueConfig,
  fields : Array[String],
) -> UniqueConfig {
  { ..self, fields }
}

///|
/// Compute the Redis uniqueness key for `job` using `config`.
/// The key is a colon-joined string of the selected job fields, prefixed with
/// "tide:unique:".  It is safe to pass the result directly to
/// RedisEngine::insert_unique_job as the `unique_key` argument.
pub fn compute_unique_key(job : Job, config : UniqueConfig) -> String {
  let parts : Array[String] = []
  for field in config.fields {
    match field {
      "queue" => parts.push(job.queue)
      "worker" => parts.push(job.worker)
      "args" => parts.push(job.args)
      "tags" => parts.push(json_string_array(job.tags))
      _ => ()
    }
  }
  "tide:unique:" + parts.join(":")
}
