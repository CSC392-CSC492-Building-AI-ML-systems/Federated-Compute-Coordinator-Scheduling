"""Own shared state and its lock; services define each complete update boundary."""

import asyncio

from coordinator.models.job import JobStatus
from coordinator.models.lease import TERMINAL_LEASE_STATUSES, Lease
from coordinator.models.provider import Provider


class RecordTable(dict):
    """
    Records by id, the same as a plain dict.
    It also keeps the ids of records that have not finished, oldest first,
    so a loop that only cares about open work does not look through every record ever made.
    """

    # The statuses a record never leaves. Each table below names its own.
    finished_statuses: frozenset = frozenset()

    def __init__(self):
        super().__init__()
        self._unfinished_ids: dict[str, None] = {}

    def __setitem__(self, record_id: str, record) -> None:
        super().__setitem__(record_id, record)
        # A dict keeps the order records came in, and storing the same id again adds nothing.
        self._unfinished_ids[record_id] = None

    def update(self, *args, **kwargs):
        # update, setdefault and |= store records without calling __setitem__,
        # so the lists would never see them. Refusing is safer than losing work quietly.
        raise TypeError("Store a record with table[record_id] = record")

    def setdefault(self, *args, **kwargs):
        raise TypeError("Invalid insertion method")

    def __ior__(self, *args, **kwargs):
        raise TypeError("Invalid insertion method")

    def unfinished(self) -> list:
        """Every record that has not finished, oldest first."""
        return self._keep_unfinished(self._unfinished_ids)

    def _keep_unfinished(self, ids: dict[str, None]) -> list:
        """The records in ids that have not finished. The rest are forgotten."""
        unfinished = []
        for record_id in list(ids):
            record = self.get(record_id)
            # 1, the record was taken out of the table -> forget it
            # 2, the record finished -> forget it, a finished record never opens again
            # 3, otherwise -> keep it
            if record is None or record.status in self.finished_statuses:
                del ids[record_id]
            else:
                unfinished.append(record)
        return unfinished


class JobTable(RecordTable):
    """Every job by id. A COMPLETED or FAILED job never changes again."""

    finished_statuses = frozenset({JobStatus.COMPLETED, JobStatus.FAILED})


class LeaseTable(RecordTable):
    """
    Every lease by id.
    It also keeps a short list of lease ids for each provider,
    so a provider asking for its offers does not look through every lease ever made.
    """

    finished_statuses = TERMINAL_LEASE_STATUSES

    def __init__(self):
        super().__init__()
        self._ids_by_provider: dict[str, dict[str, None]] = {}

    def __setitem__(self, lease_id: str, lease: Lease) -> None:
        super().__setitem__(lease_id, lease)
        # A lease keeps its provider for life, so it is filed once, when it is stored.
        self._ids_by_provider.setdefault(lease.provider_id, {})[lease_id] = None

    def unfinished_for(self, provider_id: str) -> list[Lease]:
        """This provider's leases that have not finished, oldest first."""
        return self._keep_unfinished(self._ids_by_provider.get(provider_id, {}))


class Store:
    """
    Looks like a transaction.... A complete usecase (worst case) would modify 3 of them,
    a single lock is needed.
    """

    def __init__(self):
        self.providers: dict[str, Provider] = {}
        self.jobs: JobTable = JobTable()
        self.leases: LeaseTable = LeaseTable()
        self.lock = asyncio.Lock()
