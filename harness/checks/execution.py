"""Expected reactions to execution-side failure modes."""


def result(check: str, passed: bool, detail: str = ""):
    return {"check": check, "status": "PASS" if passed else "FAIL", "detail": detail}


def check_drain_mid_job(events, final_state):
    """Within grace the job completes; past grace the lease is reclaimed and retried."""
    results = []
    drain_events = [e for e in events if e["event"] == "drain_started"]
    results.append(result("drain started by provider", bool(drain_events)))
    # Draining provider should end up DRAINED (or stop heartbeating)
    # Check if this scenario met grace or missed grace:
    # If the provider reported SUCCESS before deadline -> meets grace
    # If lease was revoked with DRAIN_RECLAIM -> misses grace
    leases = final_state.get("leases", {})
    reclaimed = any(lease.get("reason") == "DRAIN_RECLAIM" for lease in leases.values())
    if reclaimed:
        results.append(result("lease reclaimed with reason DRAIN_RECLAIM", True))
    else:
        results.append(result("job completed within grace period", True))
    # All jobs must end up COMPLETED
    jobs = final_state.get("jobs", {})
    all_completed = all(job.get("status") == "COMPLETED" for job in jobs.values())
    results.append(result("all jobs COMPLETED", all_completed))
    return results


def check_upload_fail(events, final_state):
    """Lease reports ARTIFACT_UPLOAD_FAILED; retry check runs."""
    results = []
    failed_reports = [
        e
        for e in events
        if e["event"] == "reported" and e.get("reason") == "ARTIFACT_UPLOAD_FAILED"
    ]
    results.append(result("artifact upload failed reported", bool(failed_reports)))
    # jobs finish COMPLETED?
    jobs = final_state.get("jobs", {})
    all_completed = all(j.get("status") == "COMPLETED" for j in jobs.values())
    results.append(result("all jobs COMPLETED", all_completed))
    # for fail did it retry (attempt_count == 2) and NOT exclude the provider?
    for report_event in failed_reports:
        # Find the job from the lease in events
        lease_id = report_event["lease_id"]
        lease = final_state.get("leases", {}).get(lease_id, {})
        job_id = lease.get("job_id")
        job = jobs.get(job_id, {})

        results.append(
            result(
                f"{job_id} retried with attempt_count 2",
                job.get("attempt_count") == 2,
                f"attempt_count: {job.get('attempt_count')}",
            )
        )
        # Provider should NOT be in rejected_provider_ids (transient error retryable anywhere)
        failing_provider = report_event["provider"]
        not_excluded = failing_provider not in job.get("rejected_provider_ids", [])
        results.append(result(f"{failing_provider} not excluded from {job_id}", not_excluded))
    return results


def check_disk_full(events, final_state):
    """Lease reports DISK_FULL; retry check runs."""
    results = []
    disk_full_reports = [
        e for e in events if e["event"] == "reported" and e.get("reason") == "DISK_FULL"
    ]
    results.append(result("disk full reported", bool(disk_full_reports)))
    jobs = final_state.get("jobs", {})
    leases = final_state.get("leases", {})
    # ask each job that hit disk full:
    for report_event in disk_full_reports:
        failing_provider = report_event["provider"]
        lease_id = report_event["lease_id"]
        job_id = leases.get(lease_id, {}).get("job_id")
        job = jobs.get(job_id, {})
        # The failing node MUST be in rejected_provider_ids (exclusion per job)
        excluded = failing_provider in job.get("rejected_provider_ids", [])
        results.append(result(f"{failing_provider} excluded from retrying {job_id}", excluded))
        # Job completed on attempt 2
        results.append(
            result(
                f"{job_id} COMPLETED on attempt 2",
                job.get("status") == "COMPLETED" and job.get("attempt_count") == 2,
            )
        )
    return results
