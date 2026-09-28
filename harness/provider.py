"""One simulated provider node. `behavior` selects normal operation or a failure mode.

The normal lifecycle lives here. At the step a failure mode changes, the provider
branches on `behavior` and calls the matching function in harness.behaviors.
"""


class FakeProvider:
    # TODO: Agree on constructor inputs (spec, client, behavior, params).

    async def run(self):
        """Register, then heartbeat and handle lease offers until stopped."""
        raise NotImplementedError("Team implementation pending")
