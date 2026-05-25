// LocalPeer: single-node peer that is always the leader.
// Use when running a single instance without multi-node coordination.
// No Redis operations are performed; no background renewal loop is needed.

///|
pub struct LocalPeer {
  _unit : Unit
}

///|
pub fn LocalPeer::new() -> LocalPeer {
  { _unit: () }
}

///|
pub fn LocalPeer::is_leader(self : LocalPeer) -> Bool {
  ignore(self)
  true
}
