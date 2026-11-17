"""Checks for expected reactions to failurees caused by heartbeat variances.

events: every provider's event log from the run (dicts with "provider", "event", ...).
final_state: the coordinator's records at the end of the run:
    {"status": GET /status response, "jobs": {job_id: record}, "leases": {lease_id: record}}
Each check returns a list of results: {"check": ..., "status": "PASS" or "FAIL", "detail": ...}.

#TODO: The coordinator has no GET /providers/{id} yet, so provider states come from the
/status counts. This works while each scenario has one provider per expected state.
"""


def result(check, passed, detail):
    """One check result. `passed` is a bool, stored as "PASS" or "FAIL"."""
    return {"check": check, "status": "PASS" if passed else "FAIL", "detail": detail}


def record(final_state, kind, record_id):
    """Return a record from final_state, or {} if the run did not fetch it."""
    return final_state.get(kind, {}).get(record_id, {})


def provider_counts(final_state):
    """Return how many providers the coordinator has in each status,
    e.g. {"ACTIVE": 1, "STALE": 1}."""
    return final_state.get("status", {}).get("providers", {})


def registered_providers(events):
    """Return IDs of every provider that registered."""
    return [e["provider"] for e in events if e["event"] == "registered"]


def providers_with_behavior(events, behavior):
    """Return IDs of the providers that registered with this behavior."""
    return [
        e["provider"]
        for e in events
        if e["event"] == "registered" and e.get("behavior") == behavior
    ]


def unfinished_leases(events, provider_id):
    """Return leases this provider accepted but never reported."""
    accepted = [
        e["lease_id"] for e in events if e["provider"] == provider_id and e["event"] == "accepted"
    ]
    reported = [
        e["lease_id"] for e in events if e["provider"] == provider_id and e["event"] == "reported"
    ]
    return [lease_id for lease_id in accepted if lease_id not in reported]


def check_disappear(events, final_state):
    """Provider becomes STALE; its leases are revoked; jobs retry elsewhere."""
    vanished = providers_with_behavior(events, "disappear")
    if not vanished:
        return [result("disappear provider in run", False, "no provider used disappear")]

    counts = provider_counts(final_state)
    stale = counts.get("STALE", 0)
    results = [
        result(
            f"{len(vanished)} provider(s) STALE",
            stale == len(vanished),
            f"provider counts {counts}",
        )
    ]

    for provider_id in vanished:
        for lease_id in unfinished_leases(events, provider_id):
            lease = record(final_state, "leases", lease_id)
            revoked = lease.get("status") == "REVOKED" and lease.get("reason") == "PROVIDER_STALE"
            results.append(
                result(
                    f"{lease_id} revoked for PROVIDER_STALE",
                    revoked,
                    f"status {lease.get('status')}, reason {lease.get('reason')}",
                )
            )

            job_id = lease.get("job_id")
            job_status = record(final_state, "jobs", job_id).get("status")
            results.append(
                result(
                    f"{job_id} still COMPLETED by another provider",
                    job_status == "COMPLETED",
                    f"status {job_status}",
                )
            )
    return results


def check_clock_skew(events, final_state):
    """Makes sure that skewed provider time does not affect stale detection."""
    skewed = providers_with_behavior(events, "clock_skew")
    if not skewed:
        return [result("clock_skew provider in run", False, "no provider used clock_skew")]

    counts = provider_counts(final_state)
    total = len(registered_providers(events))
    results = [
        result(
            f"all {total} providers ACTIVE",
            counts.get("ACTIVE", 0) == total,
            f"provider counts {counts}",
        )
    ]

    stale_leases = [
        lease_id
        for lease_id, lease in final_state.get("leases", {}).items()
        if lease.get("reason") == "PROVIDER_STALE"
    ]
    results.append(
        result("no lease revoked for PROVIDER_STALE", not stale_leases, f"revoked: {stale_leases}")
    )

    for job_id, job in final_state.get("jobs", {}).items():
        first_try = job.get("status") == "COMPLETED" and job.get("attempt_count") == 1
        results.append(
            result(
                f"{job_id} COMPLETED on first attempt",
                first_try,
                f"status {job.get('status')}, attempt_count {job.get('attempt_count')}",
            )
        )
    return results
