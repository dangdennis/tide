// Unit tests for the cron expression parser.

///|
test "parse @daily matches midnight" {
  let expr = @cron.CronExpr::parse("@daily") catch { e => panic() }
  // 2024-03-15 00:00 UTC = 1710460800000 ms
  assert_true(expr.matches_ms(1710460800000L))
  // 2024-03-15 09:00 UTC = 1710493200000 ms — should NOT match @daily
  assert_false(expr.matches_ms(1710493200000L))
}

///|
test "parse */15 minutes" {
  let expr = @cron.CronExpr::parse("*/15 * * * *") catch { e => panic() }
  // Any day, hour — test minute 0, 15, 30, 45 match; 1, 14, 16 do not.
  // Use 2024-01-01 00:00 UTC base (ts=1704067200000)
  let base = 1704067200000L
  assert_true(expr.matches_ms(base))                // minute 0
  assert_true(expr.matches_ms(base + 15L * 60000L)) // minute 15
  assert_true(expr.matches_ms(base + 30L * 60000L)) // minute 30
  assert_true(expr.matches_ms(base + 45L * 60000L)) // minute 45
  assert_false(expr.matches_ms(base + 1L * 60000L)) // minute 1
  assert_false(expr.matches_ms(base + 14L * 60000L)) // minute 14
}

///|
test "parse specific minute and hour" {
  let expr = @cron.CronExpr::parse("30 9 * * *") catch { e => panic() }
  // 2024-01-01 09:30 UTC = 1704067200000 + 9*3600000 + 30*60000
  let ts = 1704067200000L + 9L * 3600000L + 30L * 60000L
  assert_true(expr.matches_ms(ts))
  // 2024-01-01 09:00 UTC — should not match (minute is 0 not 30)
  assert_false(expr.matches_ms(1704067200000L + 9L * 3600000L))
}

///|
test "parse range 1-5 in dow" {
  let expr = @cron.CronExpr::parse("0 9 * * 1-5") catch { e => panic() }
  // 2024-03-11 (Monday) 09:00 UTC = 1710147600000
  // 2024-03-15 (Friday) 09:00 UTC = 1710493200000
  // 2024-03-16 (Saturday) 09:00 UTC = 1710579600000
  assert_true(expr.matches_ms(1710147600000L))  // Monday
  assert_true(expr.matches_ms(1710493200000L))  // Friday
  assert_false(expr.matches_ms(1710579600000L)) // Saturday
}

///|
test "parse invalid expression raises" {
  let result = try? @cron.CronExpr::parse("not a cron")
  assert_true(result is Err(_))
}

///|
test "parse @hourly" {
  let expr = @cron.CronExpr::parse("@hourly") catch { e => panic() }
  // 2024-01-01 05:00 UTC
  let ts = 1704067200000L + 5L * 3600000L
  assert_true(expr.matches_ms(ts))
  // 2024-01-01 05:30 UTC — not at minute 0
  assert_false(expr.matches_ms(ts + 30L * 60000L))
}
