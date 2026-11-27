"""Expected reactions to execution-side failure modes."""

from datetime import datetime, timedelta


def result(check: str, passed: bool, detail: str = ""):
    return {"check": check, "status": "PASS" if passed else "FAIL", "detail": detail}


def check_drain_mid_job(events, final_state):
    """Within grace the job completes; past grace the lease is reclaimed and retried."""
    results = []
    drain_events = [e for e in events if e["event"] == "drain_started"]
    results.append(result("drain started by provider", bool(drain_events)))
    if not drain_events:
        return results
    drain_event = drain_events[0]
    provider_id = drain_event["provider"]
    provider_status = final_state.get("providers", {}).get(provider_id, {}).get("status")
    results.append(
        result(
            f"{provider_id} DRAINED",
            provider_status == "DRAINED",
            f"status={provider_status}",
        )
    )
    finishes_at_raw = drain_event.get("job_finishes_at")
    if finishes_at_raw is None:
        results.append(result("job_finishes_at recorded on drain_started", False))
        return results

    drain_time = datetime.fromisoformat(drain_event["t"])
    deadline = drain_time + timedelta(seconds=drain_event["grace_period_seconds"])
    meets_grace = datetime.fromisoformat(finishes_at_raw) < deadline
    drain_idx = events.index(drain_event)
    accepted_before_drain = [
        event
        for event in events[:drain_idx]
        if event["event"] == "accepted" and event["provider"] == provider_id
    ]
    accepted = accepted_before_drain[-1] if accepted_before_drain else None
    if accepted is None:
        results.append(result(f"{provider_id} accepted a lease before drain", False))
        return results

    leases = final_state.get("leases", {})
    jobs = final_state.get("jobs", {})
    lease = leases.get(accepted["lease_id"], {})
    job = jobs.get(accepted["job_id"], {})

    if meets_grace:
        results.append(
            result(
                "lease RELEASED with SUCCESS within grace",
                lease.get("status") == "RELEASED" and lease.get("outcome") == "SUCCESS",
                f"status={lease.get('status')}, outcome={lease.get('outcome')}",
            )
        )
        results.append(
            result(
                "job COMPLETED on attempt 1",
                job.get("status") == "COMPLETED" and job.get("attempt_count") == 1,
                f"status={job.get('status')}, attempt_count={job.get('attempt_count')}",
            )
        )
    else:
        results.append(
            result(
                "lease REVOKED with DRAIN_RECLAIM past grace",
                lease.get("status") == "REVOKED" and lease.get("reason") == "DRAIN_RECLAIM",
                f"status={lease.get('status')}, reason={lease.get('reason')}",
            )
        )
        retried_elsewhere = any(
            other_lease.get("job_id") == accepted["job_id"]
            and other_lease.get("provider_id") != provider_id
            and other_lease.get("outcome") == "SUCCESS"
            for other_lease in leases.values()
        )
        results.append(
            result(
                "job retried on another provider and COMPLETED on attempt 2",
                job.get("status") == "COMPLETED"
                and job.get("attempt_count") == 2
                and retried_elsewhere,
                f"status={job.get('status')}, attempt_count={job.get('attempt_count')}",
            )
        )
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
