// Connection pool: borrow/return with async waiting.
//
// The pool hands out exclusive ownership of a Connection and reclaims it on
// return. It never permanently shrinks: connections are opened lazily up to
// `max_size`, a dead connection is dropped on the error path (decrementing the
// live count), and the next `acquire` transparently re-opens a replacement.

///|
pub struct Pool {
  config : ClientConfig
  max_size : Int
  available : @async.Queue[Connection]
  mut live : Int // connections owned by the pool (idle in queue + borrowed out)
}

///|
pub async fn Pool::new(config : ClientConfig, size : Int) -> Pool raise RedisError {
  let available : @async.Queue[Connection] = @async.Queue(
    kind=@aqueue.Blocking(size),
  )
  let pool : Pool = { config, max_size: size, available, live: 0 }
  // Pre-open the connections (fail fast at startup if Redis is unreachable,
  // matching prior behaviour). Runtime failures are healed lazily by `acquire`.
  for _ in 0..<size {
    let conn = Connection::open(config)
    pool.available.put(conn) catch {
      _ => {
        conn.close()
        continue
      }
    }
    pool.live = pool.live + 1
  }
  pool
}

// Borrow a connection; opens one lazily if under capacity, else blocks until
// one is returned. A connection flagged closed is transparently reconnected.

///|
pub async fn Pool::acquire(self : Pool) -> Connection raise RedisError {
  // 1. Reuse an idle connection if one is ready.
  let idle = try {
    self.available.try_get()
  } catch {
    _ => None
  }
  match idle {
    Some(conn) => return self.ensure_open(conn)
    None => ()
  }
  // 2. Grow lazily up to max_size. Increment before awaiting so concurrent
  //    borrowers (cooperative scheduler) observe the reservation.
  if self.live < self.max_size {
    self.live = self.live + 1
    return Connection::open(self.config) catch {
      e => {
        self.live = self.live - 1
        raise e
      }
    }
  }
  // 3. At capacity: wait for a returned connection.
  let conn = self.available.get() catch {
    _ => raise RedisError::PoolExhausted
  }
  self.ensure_open(conn)
}

///|
/// Reconnect a borrowed connection if it is known to be dead.
async fn Pool::ensure_open(
  self : Pool,
  conn : Connection,
) -> Connection raise RedisError {
  if conn.closed {
    conn.reopen() catch {
      e => {
        // Could not heal it; it no longer counts toward the live set.
        self.live = self.live - 1
        raise e
      }
    }
  }
  conn
}

// Run an async closure with an acquired connection, returning it afterwards.
// A connection that suffered a transport failure (flagged closed) is dropped so
// the pool re-opens a replacement on the next acquire; a still-healthy
// connection (e.g. after an application-level CommandError) is returned to the
// pool. The closure may raise any error (Error).

///|
pub async fn[T] Pool::with_conn(
  self : Pool,
  f : async (Connection) -> T raise Error,
) -> T raise Error {
  let conn = self.acquire()
  try {
    let v = f(conn)
    self.release(conn)
    v
  } catch {
    e => {
      if conn.closed {
        // Transport is dead: discard so a fresh one is opened on next acquire.
        conn.close()
        self.live = self.live - 1
      } else {
        // Connection is still usable; return it to the pool.
        self.release(conn)
      }
      raise e
    }
  }
}

///|
/// Return a connection to the idle queue, closing it if the queue rejects it.
async fn Pool::release(self : Pool, conn : Connection) -> Unit {
  self.available.put(conn) catch {
    _ => {
      conn.close()
      self.live = self.live - 1
    }
  }
}
