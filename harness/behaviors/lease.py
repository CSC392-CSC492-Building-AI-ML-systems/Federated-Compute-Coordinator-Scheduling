"""Offer-side failure modes. Names and signatures are placeholders for team design."""


def reject_lease(provider, offer):
    """Provider rejects a lease (busy with local work)."""
    raise NotImplementedError("Team implementation pending")


def capability_lie(provider, offer):
    """Provider advertised capabilities it does not have (capability mismatch)."""
    raise NotImplementedError("Team implementation pending")
