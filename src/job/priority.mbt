// Named priority constants for Job::with_priority.
// Value 0 is highest urgency; 9 is lowest.  5 is the default (normal).
// Internally the score formula is:
//   priority × 1_000_000_000_000 + scheduled_at_ms
// so lower priority integers sort ahead of higher ones at the same time.

///|
pub const PRIORITY_CRITICAL : Int = 0

///|
pub const PRIORITY_HIGHEST : Int = 1

///|
pub const PRIORITY_HIGH : Int = 2

///|
pub const PRIORITY_ABOVE_NORMAL : Int = 3

///|
pub const PRIORITY_NORMAL : Int = 5

///|
pub const PRIORITY_BELOW_NORMAL : Int = 6

///|
pub const PRIORITY_LOW : Int = 7

///|
pub const PRIORITY_LOWEST : Int = 9

///|
/// Return true iff p is within the valid priority range 0–9.
pub fn priority_valid(p : Int) -> Bool {
  p >= 0 && p <= 9
}

///|
/// Like Job::with_priority but silently clamps out-of-range values into 0–9.
pub fn Job::with_priority_clamped(self : Job, p : Int) -> Job {
  let clamped = if p < 0 { 0 } else if p > 9 { 9 } else { p }
  { ..self, priority: clamped }
}
