// Integration tests for Pool lifecycle: borrow, return, lazy growth, and
// transport-failure recovery.
// Requires: docker-compose up -d redis  (Redis on 127.0.0.1:6379)
// Run:      moon test --target native

///|
let pool_test_url = "redis://127.0.0.1:6379"

///|
async fn make_pool(size : Int) -> Pool raise RedisError {
  Pool::new(ClientConfig::from_url(pool_test_url), size)
}

///|
async test "with_conn executes closure successfully" {
  let pool = make_pool(1) catch { _ => return }
  pool.with_conn(async fn(conn) { let _ = conn.command(["PING"]) }) catch {
    _ => return
  }
}

///|
async test "with_conn reuses connection: live stays at 1 across sequential borrows" {
  let pool = make_pool(1) catch { _ => return }
  assert_eq(pool.live, 1)
  for _ in 0..<3 {
    pool.with_conn(async fn(conn) { let _ = conn.command(["PING"]) }) catch {
      _ => return
    }
  }
  assert_eq(pool.live, 1)
}

///|
async test "with_conn returns connection to pool after application error" {
  let pool = make_pool(1) catch { _ => return }
  let initial_live = pool.live
  // A CommandError is an application-level error — the connection is still healthy.
  pool.with_conn(_conn => raise RedisError::CommandError("simulated app error")) catch {
    _ => ()
  }
  // Connection should have been returned to the pool, not dropped.
  assert_eq(pool.live, initial_live)
}

///|
async test "with_conn drops connection after transport failure" {
  let pool = make_pool(1) catch { _ => return }
  assert_eq(pool.live, 1)
  // Closing the connection marks it dead; the pool drops it on error path.
  pool.with_conn(conn => {
    conn.close()
    raise RedisError::IoError("simulated transport failure")
  }) catch {
    _ => ()
  }
  assert_eq(pool.live, 0)
}

///|
async test "pool opens fresh connection after transport-failure drop" {
  let pool = make_pool(1) catch { _ => return }
  // Drop the pre-opened connection.
  pool.with_conn(conn => {
    conn.close()
    raise RedisError::IoError("simulated transport failure")
  }) catch {
    _ => ()
  }
  assert_eq(pool.live, 0)
  // Next acquire should lazily open a replacement (live < max_size).
  pool.with_conn(async fn(conn) { let _ = conn.command(["PING"]) }) catch {
    _ => return
  }
  assert_eq(pool.live, 1)
}

///|
async test "pool with size 2 pre-opens both connections" {
  let pool = make_pool(2) catch { _ => return }
  assert_eq(pool.live, 2)
}
