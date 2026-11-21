"""Contract section 9 invariants, checked on every scenario run."""


def result(check: str, passed: bool, detail: str = ""):
    return {"check": check, "status": "PASS" if passed else "FAIL", "detail": detail}


# Might want to add more things.
# atmost one live lease per provider
# outcome exists iff released
# drained providers have no live leases
# rn its simple
def check_invariants(events, final_state):
    results = []
    jobs = final_state.get("jobs", {})
    leases = final_state.get("leases", {})
    # No job has more than one non-terminal lease
    active_leases_per_job = {}
    for lease in leases.values():
        if lease.get("status") in ("OFFERED", "ACTIVE"):
            job_id = lease.get("job_id")
            active_leases_per_job[job_id] = active_leases_per_job.get(job_id, 0) + 1
    no_double_leases = all(count <= 1 for count in active_leases_per_job.values())
    results.append(result("no job has multiple live leases simultaneously", no_double_leases))
    # No job exceeds max attempts
    no_exceeded_attempts = all(
        j.get("attempt_count", 0) <= j.get("max_attempts", 3) for j in jobs.values()
    )
    results.append(result("no job exceeded max_attempts", no_exceeded_attempts))
    return results
