// Connection pool: fixed-size, borrow/return with async waiting.
// The pool hands out exclusive ownership of a Connection and reclaims it on return.

///|
pub struct Pool {
  config : ClientConfig
  max_size : Int
  available : @async.Queue[Connection]
}

///|
pub async fn Pool::new(
  config : ClientConfig,
  size : Int,
) -> Pool raise RedisError {
  let available : @async.Queue[Connection] = @async.Queue(
    kind=@aqueue.Blocking(size),
  )
  for _ in 0..<size {
    let conn = Connection::open(config)
    // put can raise an unchecked error (queue closed); safe to ignore during init.
    available.put(conn) catch {
      _ => ()
    }
  }
  { config, max_size: size, available }
}

// Borrow a connection; blocks until one becomes available.

///|
pub async fn Pool::acquire(self : Pool) -> Connection raise RedisError {
  try {
    match self.available.try_get() {
      Some(conn) => conn
      None => self.available.get()
    }
  } catch {
    _ => raise RedisError::PoolExhausted
  }
}

// Run an async closure with an acquired connection, returning it afterwards.
// Closes the connection on error (pool shrinks); acceptable for Phase 1.
// The closure may raise any error (Error); RedisError from acquire() is also propagated.

///|
pub async fn[T] Pool::with_conn(
  self : Pool,
  f : async (Connection) -> T raise Error,
) -> T raise Error {
  let conn = self.acquire()
  try {
    let v = f(conn)
    // Return connection to pool; ignore put failure (queue full/closed).
    self.available.put(conn) catch {
      _ => conn.close()
    }
    v
  } catch {
    e => {
      conn.close()
      raise e
    }
  }
}
