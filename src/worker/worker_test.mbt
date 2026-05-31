// Unit tests for PerformResult variants and their Show/Debug output.

///|
test "PerformResult Debug: Ok" {
  debug_inspect(PerformResult::Ok, content="Ok")
}

///|
test "PerformResult Debug: Snooze" {
  debug_inspect(PerformResult::Snooze(30), content="Snooze(30)")
}

///|
test "PerformResult Debug: Discard" {
  debug_inspect(
    PerformResult::Discard("too slow"),
    content="Discard(\"too slow\")",
  )
}

///|
test "PerformResult Show: Ok" {
  inspect(PerformResult::Ok, content="Ok")
}

///|
test "PerformResult Show: Snooze embeds the delay" {
  inspect(PerformResult::Snooze(60), content="Snooze(60)")
}

///|
test "PerformResult Show: Discard quotes the reason" {
  inspect(
    PerformResult::Discard("invalid payload"),
    content="Discard(\"invalid payload\")",
  )
}
