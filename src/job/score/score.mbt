// Single authoritative implementation of the sorted-set score formula.
//
// score = priority × priority_epoch + scheduled_at
//
// Lower score → fetched first by Redis ZRANGEBYSCORE (ascending order).
// Priority 0 is the highest urgency; 9 is the lowest.
//
// This package is kept free of Array/String so the Why3 proof backend
// can lower it without hitting unsupported type symbols.

///|
/// Width of each priority band in milliseconds.
/// One epoch ≈ 31.7 years — far beyond any realistic scheduling horizon.
pub let priority_epoch : Int64 = 1_000_000_000_000L

///|
/// Compute the sorted-set score from a priority and a scheduled_at timestamp.
/// Both callers (Job::sorted_set_score and RedisEngine) delegate here so the
/// formula has exactly one runtime definition.
///
/// The proof mirror `score(p, t)` in score.mbtp must stay identical.
/// They live in the same package so any drift is immediately visible.
pub fn compute(priority : Int64, scheduled_at : Int64) -> Int64 {
  priority * priority_epoch + scheduled_at
}
