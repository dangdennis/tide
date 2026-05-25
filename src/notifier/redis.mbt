// RedisNotifier: optional pub/sub component for cancel signals and
// dynamic concurrency scaling.
//
// Cancel signals
// ──────────────
// When a job is cancelled at the API level (Instance::cancel_job /
// cancel_by_tag), the caller can also invoke publish_cancel so that any
// node currently executing the job learns about it quickly via the
// subscribed listener.  The executor checks is_cancelled before dispatching
// to avoid starting work that has already been abandoned.
//
// Channel layout
// ──────────────
//   tide:notify:{queue}   — cancel signals; message body = job ID
//
// Concurrency scaling
// ───────────────────
// Not yet wired — a future tick can publish {"queue":q,"concurrency":n}
// messages to a separate channel that QueueRunner listens to.

///|
pub struct RedisNotifier {
  config : @redis.ClientConfig
  channel : String
  cancelled : Ref[Map[String, Bool]] // job_id → true
}

///|
pub fn RedisNotifier::new(
  config : @redis.ClientConfig,
  channel : String,
) -> RedisNotifier {
  { config, channel, cancelled: Ref(Map([])) }
}

///|
/// Return true if a cancel signal has been received for `job_id`.
pub fn RedisNotifier::is_cancelled(self : RedisNotifier, job_id : String) -> Bool {
  self.cancelled.val.get(job_id) is Some(_)
}

///|
/// Record a cancellation locally (used by the listener loop and for testing).
pub fn RedisNotifier::mark_cancelled(
  self : RedisNotifier,
  job_id : String,
) -> Unit {
  self.cancelled.val[job_id] = true
}

///|
/// Publish a cancel signal for `job_id` to the notification channel.
/// Other nodes' listener loops will pick this up and mark the job cancelled
/// in their local state, preventing it from being started.
pub async fn RedisNotifier::publish_cancel(
  self : RedisNotifier,
  pool : @redis.Pool,
  job_id : String,
) -> Unit {
  let _ = pool.with_conn(async fn(conn) {
    let _ = conn.publish(self.channel, job_id)
  }) catch {
    _ => ()
  }
}

///|
/// Open a dedicated subscribe connection and listen for cancel signals until
/// `is_stopped` returns true or the connection drops.
/// Each message body is interpreted as a job ID and recorded locally.
///
/// This method blocks until the listener exits; call it from a spawned task.
pub async fn RedisNotifier::listen(
  self : RedisNotifier,
  is_stopped : () -> Bool,
) -> Unit {
  let conn = try @redis.Connection::open(self.config) catch {
    _ => return
  } noraise {
    c => c
  }
  try conn.subscribe([self.channel]) catch {
    _ => {
      conn.close()
      return
    }
  } noraise {
    _ => ()
  }
  while !is_stopped() {
    // Use a 1 s read timeout so we can check is_stopped periodically.
    let result = try? @async.with_timeout_opt(
      1000,
      async fn() { conn.read_pub_message() },
    )
    match result {
      Ok(None) => () // timeout; loop and re-check is_stopped
      Ok(Some((_, job_id))) => self.cancelled.val[job_id] = true
      Err(_) => break // connection error; exit
    }
  }
  conn.close()
}
