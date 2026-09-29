"""Own shared state and its lock; services define each complete update boundary."""

import asyncio

from coordinator.models.job import Job
from coordinator.models.lease import Lease
from coordinator.models.provider import Provider


class Store:
    """
    Looks like a transaction.... A complete usecase (worst case) would modify 3 of them,
    a single lock is needed.
    """

    def __init__(self):
        self.providers: dict[str, Provider] = {}
        self.jobs: dict[str, Job] = {}
        self.leases: dict[str, Lease] = {}
        self.lock = asyncio.Lock()
