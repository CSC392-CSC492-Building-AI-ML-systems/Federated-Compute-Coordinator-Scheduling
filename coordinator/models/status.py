"""Define the /status response shape here."""

from pydantic import BaseModel


class StatusSnapshot(BaseModel):
    providers: dict[str, int]
    jobs: dict[str, int]
    leases: dict[str, int]
