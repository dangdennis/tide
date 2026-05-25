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
