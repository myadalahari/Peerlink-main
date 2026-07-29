"""
peerlink.exceptions
~~~~~~~~~~~~~~~~~~~
All PeerLink-specific exception types in one place.
"""

__all__ = [
    "PeerLinkError",
    "PeerNotFound",
    "PeerTimeoutError",
    "RemoteError",
    "PayloadTooLarge",
]


class PeerLinkError(Exception):
    """Base for every PeerLink error.  Catch this to handle any library fault."""


class PeerNotFound(PeerLinkError):
    """Target peer is not in the discovery table or its TTL has elapsed."""


# Backward-compat alias kept at module level so old imports still work.
PeerNotFoundError = PeerNotFound


class PeerTimeoutError(TimeoutError, PeerLinkError):
    """No reply arrived within the caller-specified timeout window."""


class RemoteError(PeerLinkError):
    """The remote node raised an exception; message carries remote type + text."""


class PayloadTooLarge(PeerLinkError):
    """Encoded payload exceeds the safe single-datagram limit."""
