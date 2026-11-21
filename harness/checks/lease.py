"""Checks for expected reactions to offer side failure modes (reject_lease, capability_lie).

events: every provider's event log from the run, in tick order (dicts with "provider",
"event", ...). A later index means a later event.
final_state: the coordinator's records at the end of the run (see collect_final_state):
    {"status": ..., "providers": {id: record}, "jobs": {id: record}, "leases": {id: record}}
    or {"error": "..."} if collecting it failed.

Each check returns a list of results {"check": ..., "status": ..., "detail": ...}:
- PASS / FAIL: the event happened, and the coordinator reacted correctly / wrongly.
- INCONCLUSIVE: the event the check needs never happened, or a record it needs was
  not fetched, so the coordinator was not tested.
"""


def result(check, passed, detail=""):
    """One check result. `passed` is a bool, stored as "PASS" or "FAIL"."""
    return {"check": check, "status": "PASS" if passed else "FAIL", "detail": detail}


def inconclusive(check, detail):
    """A result for an expectation that could not be tested in this run."""
    return {"check": check, "status": "INCONCLUSIVE", "detail": detail}


def record(final_state, kind, record_id):
    """Return a record from final_state, or {} if the run did not fetch it."""
    return final_state.get(kind, {}).get(record_id, {})


def providers_with_behavior(events, behavior):
    """Return IDs of the providers that registered with this behavior."""
    return [
        e["provider"]
        for e in events
        if e["event"] == "registered" and e.get("behavior") == behavior
    ]


def events_of(events, provider_id, name=None):
    """Return (index, event) pairs of one provider, optionally only one event name."""
    return [
        (i, e)
        for i, e in enumerate(events)
        if e["provider"] == provider_id and (name is None or e["event"] == name)
    ]


def events_for_job(events, job_id, name=None):
    """Return (index, event) pairs that carry this job_id (accepted / rejected)."""
    return [
        (i, e)
        for i, e in enumerate(events)
        if e.get("job_id") == job_id and (name is None or e["event"] == name)
    ]


def provider_active(final_state, provider_id):
    """Result: the provider ends ACTIVE. Rejecting or a mismatch is not a liveness fault."""
    
    provider = record(final_state, "providers", provider_id)
    if not provider:
        return inconclusive(f"{provider_id} ACTIVE", "provider record not fetched")
    status = provider.get("status")
    return result(f"{provider_id} ACTIVE", status == "ACTIVE", f"status {status}")


def no_attempt_used(events, job_id, job):
    """Result: attempt_count equals the accepts the harness saw for this job (I-7).
    The harness logs "accepted" only when the accept call succeeded. With a network
    error in the run, an accept may have reached the coordinator unseen, so a
    mismatch is INCONCLUSIVE instead of FAIL."""

    check = f"{job_id}: rejection used no attempt"
    accepts = len(events_for_job(events, job_id, "accepted"))
    attempts = job.get("attempt_count")
    detail = f"attempt_count {attempts}, accepted events {accepts}"
    if attempts != accepts and any(e["event"] == "network_error" for e in events):
        return inconclusive(check, f"{detail}, network errors in run")
    return result(check, attempts == accepts, detail)


def offered_again(events, provider_id, job_id, after):
    """Lease IDs of offers of this job to this provider after event index `after`."""
    return [
        e["lease_id"]
        for i, e in events_for_job(events, job_id)
        if i > after and e["provider"] == provider_id and e["event"] in ("accepted", "rejected")
    ]


def check_reject_lease(events, final_state):
    """Lease is REJECTED without using an attempt, the rejector is excluded from the
    job, and the job is finished by another provider."""
    rejectors = providers_with_behavior(events, "reject_lease")
    if not rejectors:
        return [inconclusive("reject_lease provider in run", "no provider used reject_lease")]
    if "error" in final_state:
        return [inconclusive("final state collected", final_state["error"])]

    results = []
    for provider_id in rejectors:
        results.extend(check_rejector(events, final_state, provider_id))
    return results


def check_rejector(events, final_state, provider_id):
    """Results for one reject_lease provider."""
    rejections = events_of(events, provider_id, "rejected")
    accepted = [e["lease_id"] for _, e in events_of(events, provider_id, "accepted")]

    results = []
    if not rejections:
        results.append(
            inconclusive(
                f"{provider_id} rejected an offer", "no rejected event, rejection not tested"
            )
        )
    results.append(result(f"{provider_id} accepted no lease", not accepted, f"accepted {accepted}"))
    results.append(provider_active(final_state, provider_id))

    for index, rejection in rejections:
        lease_id, job_id, reason = rejection["lease_id"], rejection["job_id"], rejection["reason"]
        lease = record(final_state, "leases", lease_id)
        job = record(final_state, "jobs", job_id)
        if not lease or not job:
            results.append(
                inconclusive(
                    f"{provider_id}: {lease_id} checked",
                    f"lease fetched {bool(lease)}, job fetched {bool(job)}",
                )
            )
            continue

        results.append(
            result(
                f"{provider_id}: {lease_id} REJECTED with {reason}",
                lease.get("status") == "REJECTED"
                and lease.get("reason") == reason
                and lease.get("outcome") is None,
                f"status {lease.get('status')}, reason {lease.get('reason')}, "
                f"outcome {lease.get('outcome')}",
            )
        )
        excluded = job.get("rejected_provider_ids", [])
        results.append(
            result(
                f"{provider_id} in {job_id} rejected_provider_ids",
                provider_id in excluded,
                f"rejected_provider_ids {excluded}",
            )
        )
        again = offered_again(events, provider_id, job_id, index)
        results.append(
            result(f"{provider_id} not offered {job_id} again", not again, f"later offers {again}")
        )
        results.append(no_attempt_used(events, job_id, job))
        results.append(
            result(
                f"{job_id} COMPLETED by another provider",
                job.get("status") == "COMPLETED",
                f"status {job.get('status')}",
            )
        )
    return results


def check_capability_lie(events, final_state):
    """A job the provider cannot really run fails with CAPABILITY_MISMATCH, the provider
    is excluded from that job, and the job is retried and finished elsewhere."""
    liars = providers_with_behavior(events, "capability_lie")
    if not liars:
        return [inconclusive("capability_lie provider in run", "no provider used capability_lie")]
    if "error" in final_state:
        return [inconclusive("final state collected", final_state["error"])]

    results = []
    for provider_id in liars:
        results.extend(check_liar(events, final_state, provider_id))
    return results


def check_liar(events, final_state, provider_id):
    """Results for one capability_lie provider."""
    # planned_outcome is the behavior's own decision: FAILURE exactly when the job
    # needs more VRAM than the provider really has.
    accepted = events_of(events, provider_id, "accepted")
    reports = {e["lease_id"]: (i, e) for i, e in events_of(events, provider_id, "reported")}
    mismatches = [(i, e) for i, e in accepted if e.get("planned_outcome") == "FAILURE"]
    honest = [(i, e) for i, e in accepted if e.get("planned_outcome") == "SUCCESS"]

    results = []
    tested = [
        (i, e)
        for i, e in mismatches
        if reports.get(e["lease_id"], (None, {}))[1].get("reason") == "CAPABILITY_MISMATCH"
    ]
    if not tested:
        results.append(
            inconclusive(
                f"{provider_id} reported CAPABILITY_MISMATCH",
                "no mismatch report, retry elsewhere not tested",
            )
        )
    results.append(provider_active(final_state, provider_id))

    for _, accept in mismatches:
        if accept["lease_id"] not in reports:
            results.append(
                inconclusive(
                    f"{provider_id}: {accept['lease_id']} reported",
                    "accepted a job it cannot run but never reported it",
                )
            )

    for accept_index, accept in tested:
        report_index = reports[accept["lease_id"]][0]
        results.extend(
            check_mismatch_lease(
                events, final_state, provider_id, accept, accept_index, report_index
            )
        )

    for _, accept in honest:
        if accept["lease_id"] in reports:
            results.append(check_honest_lease(final_state, provider_id, accept, reports))
    return results


def check_mismatch_lease(events, final_state, provider_id, accept, accept_index, report_index):
    """Results for one lease the liar reported as CAPABILITY_MISMATCH."""
    lease_id, job_id = accept["lease_id"], accept["job_id"]
    lease = record(final_state, "leases", lease_id)
    job = record(final_state, "jobs", job_id)
    if not lease or not job:
        return [
            inconclusive(
                f"{provider_id}: {lease_id} checked",
                f"lease fetched {bool(lease)}, job fetched {bool(job)}",
            )
        ]

    results = [
        result(
            f"{provider_id}: {lease_id} RELEASED with CAPABILITY_MISMATCH",
            lease.get("status") == "RELEASED"
            and lease.get("outcome") == "FAILURE"
            and lease.get("reason") == "CAPABILITY_MISMATCH",
            f"status {lease.get('status')}, outcome {lease.get('outcome')}, "
            f"reason {lease.get('reason')}",
        )
    ]
    excluded = job.get("rejected_provider_ids", [])
    results.append(
        result(
            f"{provider_id} in {job_id} rejected_provider_ids",
            provider_id in excluded,
            f"rejected_provider_ids {excluded}",
        )
    )
    again = offered_again(events, provider_id, job_id, report_index)
    results.append(
        result(f"{provider_id} not offered {job_id} again", not again, f"later offers {again}")
    )

    retries = [
        e
        for i, e in events_for_job(events, job_id, "accepted")
        if i > report_index and e["provider"] != provider_id and e["lease_id"] != lease_id
    ]
    results.append(
        result(
            f"{job_id} retried on another provider",
            bool(retries),
            f"later accepts {[(e['provider'], e['lease_id']) for e in retries]}",
        )
    )
    results.append(
        result(
            f"{job_id} COMPLETED after a retry",
            job.get("status") == "COMPLETED" and (job.get("attempt_count") or 0) >= 2,
            f"status {job.get('status')}, attempt_count {job.get('attempt_count')}",
        )
    )

    # last_failure_reason is only this lease's reason if no later attempt failed.
    # A later attempt counts as failed unless its provider reported SUCCESS.
    successes = {
        e["lease_id"] for e in events if e["event"] == "reported" and e.get("outcome") == "SUCCESS"
    }
    later_attempts = [
        e["lease_id"] for i, e in events_for_job(events, job_id, "accepted") if i > accept_index
    ]
    if all(later in successes for later in later_attempts):
        results.append(
            result(
                f"{job_id} last_failure_reason CAPABILITY_MISMATCH",
                job.get("last_failure_reason") == "CAPABILITY_MISMATCH",
                f"last_failure_reason {job.get('last_failure_reason')}",
            )
        )
    return results


def check_honest_lease(final_state, provider_id, accept, reports):
    """Result for a reported lease on a job the liar can really run."""
    lease_id = accept["lease_id"]
    reported = reports[lease_id][1]
    lease = record(final_state, "leases", lease_id)
    if not lease:
        return inconclusive(f"{provider_id}: {lease_id} checked", "lease record not fetched")
    return result(
        f"{provider_id}: {lease_id} SUCCESS on a job it can run",
        reported.get("outcome") == "SUCCESS"
        and lease.get("status") == "RELEASED"
        and lease.get("outcome") == "SUCCESS",
        f"reported {reported.get('outcome')}, status {lease.get('status')}, "
        f"outcome {lease.get('outcome')}",
    )
