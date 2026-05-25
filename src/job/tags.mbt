// Tag helpers: membership check and list filtering.

///|
pub fn job_has_tag(job : Job, tag : String) -> Bool {
  for t in job.tags {
    if t == tag {
      return true
    }
  }
  false
}

///|
pub fn jobs_with_tag(jobs : Array[Job], tag : String) -> Array[Job] {
  let result : Array[Job] = []
  for job in jobs {
    if job_has_tag(job, tag) {
      result.push(job)
    }
  }
  result
}
