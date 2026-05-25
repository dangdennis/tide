// ShutdownSignal is a one-shot signal that coordinates graceful shutdown.
// send() is synchronous; wait() is async and blocks until send() is called.
// is_stopped() reflects whether send() has been called.

///|
pub(all) struct ShutdownSignal {
  cond : @cond_var.Cond
  stopped : Ref[Bool]
}

///|
pub fn ShutdownSignal::new() -> ShutdownSignal {
  { cond: @cond_var.Cond(), stopped: Ref(false) }
}

// Blocks until send() is called. Safe to call before send() (will block),
// or after (returns immediately). Cooperative scheduler prevents races.

///|
pub async fn ShutdownSignal::wait(self : ShutdownSignal) -> Unit {
  if !self.stopped.val {
    self.cond.wait()
  }
  self.stopped.val = true
}

// Non-blocking. May be called from non-async contexts (e.g. signal handlers).

///|
pub fn ShutdownSignal::send(self : ShutdownSignal) -> Unit {
  self.stopped.val = true
  self.cond.signal()
}

///|
pub fn ShutdownSignal::is_stopped(self : ShutdownSignal) -> Bool {
  self.stopped.val
}
