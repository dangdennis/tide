// LocalPeer: single-node peer that is always the leader.
// Use when running a single instance without multi-node coordination.
// No Redis operations are performed; no background renewal loop is needed.

///|
/// Single-node peer stub — always reports itself as leader.
/// Use this (the default) when running a single tide process that does not
/// need Redis-backed leader election.
pub struct LocalPeer {
  _unit : Unit
}

///|
/// Create a new `LocalPeer`.  No Redis connection is required.
pub fn LocalPeer::new() -> LocalPeer {
  { _unit: () }
}

///|
/// Always returns `true`; a single-node instance is always the leader.
pub fn LocalPeer::is_leader(self : LocalPeer) -> Bool {
  ignore(self)
  true
}
