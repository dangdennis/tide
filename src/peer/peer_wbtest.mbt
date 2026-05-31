// Whitebox tests — access internal RedisPeer fields directly.

///|
async test "RedisPeer election_key uses tide:peer: prefix" {
  let peer : RedisPeer = {
    pool: @redis.Pool::new(@redis.ClientConfig::default(), 0) catch {
      _ => abort("pool should succeed with size 0")
    },
    name: "myapp",
    node_id: "node-123",
    leader: Ref(false),
  }
  assert_eq(peer.election_key(), "tide:peer:myapp")
}

///|
async test "RedisPeer election_key embeds the name" {
  let peer : RedisPeer = {
    pool: @redis.Pool::new(@redis.ClientConfig::default(), 0) catch {
      _ => abort("pool init failed")
    },
    name: "payments",
    node_id: "n1",
    leader: Ref(false),
  }
  assert_eq(peer.election_key(), "tide:peer:payments")
}

///|
async test "RedisPeer is_leader reflects the internal Ref" {
  let peer : RedisPeer = {
    pool: @redis.Pool::new(@redis.ClientConfig::default(), 0) catch {
      _ => abort("pool init failed")
    },
    name: "x",
    node_id: "n",
    leader: Ref(false),
  }
  assert_false(peer.is_leader())
  peer.leader.val = true
  assert_true(peer.is_leader())
}

///|
async test "RedisPeer becomes leader after tick against live Redis" {
  let config = @redis.ClientConfig::from_url("redis://127.0.0.1:6379") catch {
    _ => return
  }
  let pool = @redis.Pool::new(config, 1) catch { _ => return }
  let peer = RedisPeer::new(pool, "test-peer-election", "node-test-1")
  let key = peer.election_key()
  // Clean up any leftover election key from a previous run.
  pool.with_conn(async fn(conn) { let _ = conn.del(key) }) catch {
    _ => ()
  }
  peer.tick()
  assert_true(peer.is_leader())
  // Clean up so re-runs start fresh.
  pool.with_conn(async fn(conn) { let _ = conn.del(key) }) catch {
    _ => ()
  }
}
