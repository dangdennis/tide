// Tests for ClientConfig::from_url URL parser.

test "default redis URL" {
  let config = ClientConfig::from_url("redis://127.0.0.1:6379")
  assert_eq!(config.host, "127.0.0.1")
  assert_eq!(config.port, 6379)
  assert_eq!(config.db, 0)
  assert_eq!(config.password, None)
}

test "URL with password, custom port, and db" {
  let config = ClientConfig::from_url("redis://:secret@myhost:1234/2")
  assert_eq!(config.host, "myhost")
  assert_eq!(config.port, 1234)
  assert_eq!(config.db, 2)
  assert_eq!(config.password, Some("secret"))
}

test "URL with host only" {
  let config = ClientConfig::from_url("redis://myhost")
  assert_eq!(config.host, "myhost")
  assert_eq!(config.port, 6379)
  assert_eq!(config.db, 0)
  assert_eq!(config.password, None)
}

test "URL with host and db only" {
  let config = ClientConfig::from_url("redis://myhost/3")
  assert_eq!(config.host, "myhost")
  assert_eq!(config.port, 6379)
  assert_eq!(config.db, 3)
  assert_eq!(config.password, None)
}

test "URL with host and port no db" {
  let config = ClientConfig::from_url("redis://myhost:9999")
  assert_eq!(config.host, "myhost")
  assert_eq!(config.port, 9999)
  assert_eq!(config.db, 0)
}

test "URL with non-numeric db falls back to 0" {
  let config = ClientConfig::from_url("redis://myhost/notanumber")
  assert_eq!(config.db, 0)
}

test "invalid scheme raises ConnectionFailed" {
  let raised = try {
    let _ = ClientConfig::from_url("postgres://localhost")
    false
  } catch {
    RedisError::ConnectionFailed(_) => true
    _ => false
  }
  assert_true!(raised)
}

test "plain string raises ConnectionFailed" {
  let raised = try {
    let _ = ClientConfig::from_url("localhost:6379")
    false
  } catch {
    RedisError::ConnectionFailed(_) => true
    _ => false
  }
  assert_true!(raised)
}
