// Tests for tag membership and list filtering.

///|
test "job_has_tag returns true when tag is present" {
  let job = Job::new("q", "W").with_tags(["urgent", "email"])
  assert_true(job_has_tag(job, "urgent"))
  assert_true(job_has_tag(job, "email"))
}

///|
test "job_has_tag returns false when tag is absent" {
  let job = Job::new("q", "W").with_tags(["email"])
  assert_false(job_has_tag(job, "urgent"))
}

///|
test "job_has_tag returns false for empty tag list" {
  let job = Job::new("q", "W")
  assert_false(job_has_tag(job, "anything"))
}

///|
test "job_has_tag is case-sensitive" {
  let job = Job::new("q", "W").with_tags(["Urgent"])
  assert_false(job_has_tag(job, "urgent"))
}

///|
test "jobs_with_tag returns only jobs carrying the tag" {
  let j1 = Job::new("q", "W").with_tags(["urgent"])
  let j2 = Job::new("q", "W").with_tags(["normal"])
  let j3 = Job::new("q", "W").with_tags(["urgent", "normal"])
  let filtered = jobs_with_tag([j1, j2, j3], "urgent")
  assert_eq(filtered.length(), 2)
}

///|
test "jobs_with_tag returns empty array when no job matches" {
  let j1 = Job::new("q", "W").with_tags(["a"])
  let j2 = Job::new("q", "W").with_tags(["b"])
  let filtered = jobs_with_tag([j1, j2], "c")
  assert_eq(filtered.length(), 0)
}

///|
test "jobs_with_tag on empty array returns empty array" {
  let filtered = jobs_with_tag([], "x")
  assert_eq(filtered.length(), 0)
}

///|
test "jobs_with_tag does not modify the input array" {
  let jobs = [Job::new("q", "W").with_tags(["a"]), Job::new("q", "W").with_tags(["b"])]
  let _ = jobs_with_tag(jobs, "a")
  assert_eq(jobs.length(), 2)
}
