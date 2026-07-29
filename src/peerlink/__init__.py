"""
PeerLink — Zero-config P2P RPC over LAN/WiFi.

Quick start::

    from peerlink import PeerLink

    with PeerLink("NodeA") as node:
        node.register("add", lambda a, b: a + b)
        node.wait_for_peers(1)
        result = node.peer("NodeB").add(3, 4)     # UDP (auto)
        result = node.peer("NodeB").add(3, 4, _transport="tcp")  # TCP

Async::

    from peerlink import AsyncPeerLink

    async with AsyncPeerLink("Player1") as node:
        node.register("tick", game_tick)
        await node.wait_for_peers(1)
        state = await node.peer("Host").tick(inputs)

Public API summary
------------------
PeerLink            Core node (sync, context manager).
SwarmNode           Auto-starting convenience wrapper.
AsyncPeerLink       Asyncio context manager; RPC runs in thread pool.
PeerProxy           Attribute-based proxy returned by node.peer().
AsyncPeerProxy      Async counterpart to PeerProxy.
current_peer        ContextVar: caller name inside an RPC handler.
run_with_current_peer  Helper for cross-thread current_peer propagation.

Exceptions: PeerLinkError, PeerNotFound, PeerNotFoundError,
            PeerTimeoutError, RemoteError, PayloadTooLarge.

Constants:  DISCOVERY_WAIT, MAX_SAFE_UDP_PAYLOAD, RPC_TIMEOUT.
"""

from .async_node import AsyncPeerLink, AsyncPeerProxy
from .constants import DISCOVERY_WAIT, MAX_SAFE_UDP_PAYLOAD, RPC_TIMEOUT, __version__
from .exceptions import (
    PayloadTooLarge,
    PeerLinkError,
    PeerNotFound,
    PeerNotFoundError,
    PeerTimeoutError,
    RemoteError,
)
from .node import PeerLink, PeerProxy, SwarmNode
from ._utils import current_peer, run_with_current_peer

__all__ = [
    # Nodes
    "PeerLink",
    "SwarmNode",
    "AsyncPeerLink",
    # Proxies
    "PeerProxy",
    "AsyncPeerProxy",
    # Exceptions
    "PeerLinkError",
    "PeerNotFound",
    "PeerNotFoundError",
    "PeerTimeoutError",
    "RemoteError",
    "PayloadTooLarge",
    # Context
    "current_peer",
    "run_with_current_peer",
    # Constants
    "DISCOVERY_WAIT",
    "MAX_SAFE_UDP_PAYLOAD",
    "RPC_TIMEOUT",
    "__version__",
]
