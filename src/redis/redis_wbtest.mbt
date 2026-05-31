// Integration tests for Connection's typed command helpers.
// Requires: docker-compose up -d redis  (Redis on 127.0.0.1:6379)
// Run:      moon test --target native

///|
let test_url = "redis://127.0.0.1:6379"

///|
async fn open_conn() -> Connection raise RedisError {
  Connection::open(ClientConfig::from_url(test_url))
}

// ── String commands ───────────────────────────────────────────────────────────

///|
async test "set and get round-trip (ASCII)" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:str-ascii"
  let _ = conn.del(key) catch { _ => 0L }
  conn.set(key, "hello")
  let v = conn.get(key)
  debug_inspect(v, content="Some(\"hello\")")
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

///|
async test "set and get round-trip (non-ASCII key and value)" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:café"
  let _ = conn.del(key) catch { _ => 0L }
  conn.set(key, "€5")
  let v = conn.get(key)
  debug_inspect(v, content="Some(\"€5\")")
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

///|
async test "get returns None for missing key" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:get-none"
  let _ = conn.del(key) catch { _ => 0L }
  let v = conn.get(key)
  debug_inspect(v, content="None")
  conn.close()
}

///|
async test "del removes existing key and returns 1" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:del-exists"
  conn.set(key, "v")
  let n = conn.del(key)
  assert_eq(n, 1L)
  conn.close()
}

///|
async test "del on missing key returns 0" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:del-missing"
  let _ = conn.del(key) catch { _ => 0L }
  let n = conn.del(key)
  assert_eq(n, 0L)
  conn.close()
}

///|
async test "exists returns true for existing key" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:exists-true"
  conn.set(key, "v")
  assert_true(conn.exists(key))
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

///|
async test "exists returns false for missing key" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:exists-false"
  let _ = conn.del(key) catch { _ => 0L }
  assert_false(conn.exists(key))
  conn.close()
}

///|
async test "set_nx_px creates key when absent and returns true" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:nx-new"
  let _ = conn.del(key) catch { _ => 0L }
  let created = conn.set_nx_px(key, "first", 60000L)
  assert_true(created)
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

///|
async test "set_nx_px skips existing key and returns false" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:nx-exists"
  conn.set(key, "original")
  let created = conn.set_nx_px(key, "second", 60000L)
  assert_false(created)
  let v = conn.get(key)
  debug_inspect(v, content="Some(\"original\")")
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

// ── Hash commands ─────────────────────────────────────────────────────────────

///|
async test "hset and hget round-trip" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:hash"
  let _ = conn.del(key) catch { _ => 0L }
  let _ = conn.hset(key, ["name", "alice"])
  let v = conn.hget(key, "name")
  debug_inspect(v, content="Some(\"alice\")")
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

///|
async test "hgetall returns all fields as flat key-value pairs" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:hash-all"
  let _ = conn.del(key) catch { _ => 0L }
  let _ = conn.hset(key, ["a", "1", "b", "2"])
  let pairs = conn.hgetall(key)
  assert_eq(pairs.length(), 4)
  assert_eq(pairs.length() % 2, 0)
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

///|
async test "hget returns None for missing field" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:hash-field-missing"
  let _ = conn.del(key) catch { _ => 0L }
  let _ = conn.hset(key, ["a", "1"])
  let v = conn.hget(key, "z")
  debug_inspect(v, content="None")
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

// ── Sorted set commands ───────────────────────────────────────────────────────

///|
async test "zadd and zpopmin return lowest-score member first" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:zset"
  let _ = conn.del(key) catch { _ => 0L }
  let _ = conn.zadd(key, 10L, "alpha")
  let _ = conn.zadd(key, 5L, "beta")
  let popped = conn.zpopmin(key, 1)
  // ZPOPMIN returns [member, score, ...]; "beta" has the lower score.
  assert_eq(popped.length(), 2)
  assert_eq(popped[0], "beta")
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

///|
async test "zrangebyscore returns members within score range" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:zrange"
  let _ = conn.del(key) catch { _ => 0L }
  let _ = conn.zadd(key, 1L, "low")
  let _ = conn.zadd(key, 5L, "mid")
  let _ = conn.zadd(key, 10L, "high")
  let members = conn.zrangebyscore(key, "2", "7")
  assert_eq(members.length(), 1)
  assert_eq(members[0], "mid")
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

///|
async test "zrem removes member and returns 1" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:zrem"
  let _ = conn.del(key) catch { _ => 0L }
  let _ = conn.zadd(key, 1L, "m1")
  let n = conn.zrem(key, "m1")
  assert_eq(n, 1L)
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

// ── Stream commands ───────────────────────────────────────────────────────────

///|
async test "xadd returns a non-empty entry ID" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:stream-xadd"
  let _ = conn.del(key) catch { _ => 0L }
  let id = conn.xadd(key, ["msg", "hello"])
  assert_true(id.length() > 0)
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

///|
async test "xgroup_create_mkstream is idempotent (BUSYGROUP ignored)" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:stream-group"
  let _ = conn.del(key) catch { _ => 0L }
  conn.xgroup_create_mkstream(key, "grp")
  conn.xgroup_create_mkstream(key, "grp")
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

///|
async test "xreadgroup reads pending entry after xadd" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:stream-read"
  let group = "test-grp"
  let consumer = "test-consumer"
  let _ = conn.del(key) catch { _ => 0L }
  conn.xgroup_create_mkstream(key, group)
  let _ = conn.xadd(key, ["payload", "data1"])
  let entries = conn.xreadgroup(group, consumer, key, count=1, block_ms=0)
  assert_eq(entries.length(), 1)
  assert_true(entries[0].stream_id.length() > 0)
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

///|
async test "xack acknowledges pending entry and returns 1" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:stream-ack"
  let group = "test-grp"
  let consumer = "test-consumer"
  let _ = conn.del(key) catch { _ => 0L }
  conn.xgroup_create_mkstream(key, group)
  let _ = conn.xadd(key, ["k", "v"])
  let entries = conn.xreadgroup(group, consumer, key, count=1, block_ms=0)
  assert_eq(entries.length(), 1)
  let acked = conn.xack(key, group, entries[0].stream_id)
  assert_eq(acked, 1L)
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

///|
async test "xautoclaim reclaims pending entry from another consumer" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:stream-autoclaim"
  let group = "test-grp"
  let _ = conn.del(key) catch { _ => 0L }
  conn.xgroup_create_mkstream(key, group)
  let _ = conn.xadd(key, ["k", "v"])
  // consumer1 reads the entry, leaving it in the pending-entry list.
  let _ = conn.xreadgroup(group, "consumer1", key, count=1, block_ms=0)
  // consumer2 claims all pending entries idle for >= 0 ms.
  let claimed = conn.xautoclaim(key, group, "consumer2", 0L)
  assert_eq(claimed.length(), 1)
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

// ── Pub/Sub ───────────────────────────────────────────────────────────────────

///|
async test "publish returns subscriber count (0 when no subscribers)" {
  let conn = open_conn() catch { _ => return }
  let count = conn.publish("test:redis:cmd:pub-channel", "hello")
  assert_eq(count, 0L)
  conn.close()
}

// ── Misc ──────────────────────────────────────────────────────────────────────

///|
async test "scan iterates keys and finds probe key" {
  let conn = open_conn() catch { _ => return }
  let key = "test:redis:cmd:scan-probe"
  conn.set(key, "1")
  let found = Ref(false)
  let mut cur = "0"
  while true {
    let (next_cur, keys) = conn.scan(
      cur,
      match_pattern="test:redis:cmd:scan-*",
      count=100,
    )
    for k in keys {
      if k == key {
        found.val = true
      }
    }
    cur = next_cur
    if cur == "0" {
      break
    }
  }
  assert_true(found.val)
  let _ = conn.del(key) catch { _ => 0L }
  conn.close()
}

///|
async test "pipeline executes multiple commands in one round-trip" {
  let conn = open_conn() catch { _ => return }
  let k1 = "test:redis:cmd:pipe-k1"
  let k2 = "test:redis:cmd:pipe-k2"
  let _ = conn.del(k1) catch { _ => 0L }
  let _ = conn.del(k2) catch { _ => 0L }
  let results = conn.pipeline([
    ["SET", k1, "one"],
    ["SET", k2, "two"],
    ["GET", k1],
    ["DEL", k1, k2],
  ])
  assert_eq(results.length(), 4)
  assert_eq(results[2].as_string(), "one")
  conn.close()
}
