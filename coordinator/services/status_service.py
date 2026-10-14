"""Status use case: a read-only count of records per state, for tests and demos."""

from collections import Counter

from coordinator.models.job import JobStatus
from coordinator.models.lease import LeaseStatus
from coordinator.models.provider import ProviderStatus
from coordinator.models.status import StatusSnapshot
from coordinator.store import Store


def _count_by_status(records, statuses) -> dict[str, int]:
    counts = Counter(record.status for record in records)
    # Go through the enum, not the records, so states nobody is in still show as 0.
    return {status.value: counts[status] for status in statuses}


async def get_status(store: Store) -> StatusSnapshot:
    """Count Providers, Jobs and Leases by state. Never changes anything."""
    # One lock hold for all three, so the counts describe the same moment.
    async with store.lock:
        return StatusSnapshot(
            providers=_count_by_status(store.providers.values(), ProviderStatus),
            jobs=_count_by_status(store.jobs.values(), JobStatus),
            leases=_count_by_status(store.leases.values(), LeaseStatus),
        )
