"""Coordinator HTTP client. Names and signatures are placeholders for team design."""


class CoordinatorClient:
    # TODO: One method per endpoint in CSC398_API_CONTRACT.
    # Use coordinator.models enums for statuses and reasons once they exist.

    async def register_provider(self, data):
        """Register a provider."""
        raise NotImplementedError("Team implementation pending")

    async def heartbeat(self, provider_id):
        """Send a provider heartbeat."""
        raise NotImplementedError("Team implementation pending")
