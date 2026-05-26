// RedisPeer: multi-node leader election via Redis SET NX PX.
//
// Election key: tide:peer:{name}   value: {node_id}   TTL: 30 000 ms
// Renewal interval: 15 000 ms  (half the TTL, ensuring two missed renewals
// before another node can claim leadership).
//
// Algorithm per tick:
//   1. SET key node_id NX PX 30000  — claim if unowned
//   2. GET key                      — read current owner
//   3. If owner == node_id          — renew: SET key node_id XX PX 30000
//   4. Update self.leader accordingly

///|
/// Multi-node leader election peer backed by Redis SET NX PX.
/// One node holds a 30-second lease renewed every 15 seconds; other nodes
/// become leader only if the current holder fails to renew.
pub struct RedisPeer {
  pool : @redis.Pool
  name : String
  node_id : String
  leader : Ref[Bool]
}

///|
/// Create a `RedisPeer`.  `name` is the logical cluster identifier (must be
/// the same on every node); `node_id` uniquely identifies this process.
pub fn RedisPeer::new(
  pool : @redis.Pool,
  name : String,
  node_id : String,
) -> RedisPeer {
  { pool, name, node_id, leader: Ref(false) }
}

///|
/// Returns `true` if this node currently holds the leadership lease.
/// Updated after each `tick`; reads a local flag — no Redis call is made.
pub fn RedisPeer::is_leader(self : RedisPeer) -> Bool {
  self.leader.val
}

///|
/// The Redis key used for leader election in this peer group.
pub fn RedisPeer::election_key(self : RedisPeer) -> String {
  "tide:peer:\{self.name}"
}

///|
/// Perform one election/renewal attempt.  Safe to call concurrently; the
/// two-step SET NX + GET + SET XX approach is intentionally simple.
/// For sub-millisecond races, leadership may briefly oscillate; the 30 s TTL
/// ensures the window is short.
pub async fn RedisPeer::tick(self : RedisPeer) -> Unit {
  let key = self.election_key()
  let node_id = self.node_id
  let _ = self.pool.with_conn(async fn(conn) {
    // Attempt to claim if key is absent.
    let _ = conn.set_nx_px(key, node_id, 30000L)
    // Read current owner to decide whether to renew.
    match conn.get(key) {
      Some(v) =>
        if v == node_id {
          // We own the key; reset its TTL.
          let _ = conn.command(["SET", key, node_id, "XX", "PX", "30000"])
          self.leader.val = true
        } else {
          self.leader.val = false
        }
      None =>
        // Key expired between our NX attempt and GET; not leader until next tick.
        self.leader.val = false
    }
  }) catch {
    _ => self.leader.val = false
  }
}

///|
/// Run the election loop, calling tick every 15 s until `is_stopped` returns
/// true.  Clears the leader flag on clean exit to avoid stale leadership
/// being observed after the run returns.
pub async fn RedisPeer::run(self : RedisPeer, is_stopped : () -> Bool) -> Unit {
  while !is_stopped() {
    self.tick()
    @async.sleep(15000)
  }
  self.leader.val = false
}
