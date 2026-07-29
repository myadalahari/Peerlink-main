"""
peerlink.async_node
~~~~~~~~~~~~~~~~~~~
Asyncio wrapper around :class:`~peerlink.node.PeerLink`.

All blocking calls run in a thread pool via ``asyncio.to_thread`` so the
event loop stays responsive during discovery, RPC, and teardown.

Usage::

    async with AsyncPeerLink("Player1") as node:
        node.register("tick", lambda inputs: compute(inputs))
        await node.wait_for_peers(1, timeout=5)
        result = await node.peer("Host").tick(my_inputs)
        result = await node.peer("Host").tick(my_inputs, _transport="tcp")
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Optional

from .constants import RPC_TIMEOUT
from .exceptions import PeerNotFound
from .node import PeerLink, PeerProxy

__all__ = ["AsyncPeerLink", "AsyncPeerProxy"]


class AsyncPeerProxy:
    """
    Async counterpart to :class:`~peerlink.node.PeerProxy`.

    Usage::

        proxy = node.peer("Host")          # returns AsyncPeerProxy
        result = await proxy.some_rpc(arg)
        result = await proxy.some_rpc(arg, _transport="tcp")
    """

    def __init__(self, node: "AsyncPeerLink", peer_name: str) -> None:
        self._node = node
        self._name = peer_name

    async def call(
        self,
        func_name: str,
        *args: Any,
        timeout: float = RPC_TIMEOUT,
        transport: str = "auto",
        **kwargs: Any,
    ) -> Any:
        return await self._node.call(
            self._name, func_name, *args,
            timeout=timeout, transport=transport, **kwargs
        )

    def __getattr__(self, func_name: str) -> Callable[..., Any]:
        async def _caller(
            *args: Any,
            _timeout: float = RPC_TIMEOUT,
            _transport: str = "auto",
            **kw: Any,
        ) -> Any:
            return await self.call(
                func_name, *args, timeout=_timeout, transport=_transport, **kw
            )
        return _caller

    async def is_alive(self, timeout: float = 2.0) -> bool:
        try:
            res = await self.call("__ping__", timeout=timeout)
        except Exception:
            return False
        return bool(res.get("ok")) if isinstance(res, dict) else bool(res)


class AsyncPeerLink:
    """
    Async context manager around :class:`~peerlink.node.PeerLink`.

    Parameters
    ----------
    node_name:
        Human-readable identity on the LAN.
    **kwargs:
        Forwarded verbatim to :class:`~peerlink.node.PeerLink`.
    """

    def __init__(self, node_name: str, **kwargs: Any) -> None:
        self._sync = PeerLink(node_name, **kwargs)

    # ── Delegation helpers ────────────────────────────────────────────────────

    @property
    def sync(self) -> PeerLink:
        """The underlying synchronous :class:`~peerlink.node.PeerLink`."""
        return self._sync

    def register(self, name: str, func: Callable[..., Any]) -> "AsyncPeerLink":
        """Register an RPC handler (chainable; safe before start)."""
        self._sync.register(name, func)
        return self

    def set_peer_lifecycle(self, **kwargs: Any) -> "AsyncPeerLink":
        """Delegate lifecycle callbacks to the sync node (chainable)."""
        self._sync.set_peer_lifecycle(**kwargs)
        return self

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def __aenter__(self) -> "AsyncPeerLink":
        await asyncio.to_thread(self._sync.start)
        return self

    async def __aexit__(self, *_: Any) -> None:
        await asyncio.to_thread(self._sync.stop)

    # ── Async API ─────────────────────────────────────────────────────────────

    async def wait_for_peers(self, count: int = 1, timeout: float = 30.0) -> bool:
        """Non-blocking wait (runs sync poll in worker thread)."""
        return await asyncio.to_thread(self._sync.wait_for_peers, count, timeout)

    async def call(
        self,
        target: str,
        func_name: str,
        *args: Any,
        timeout: float = RPC_TIMEOUT,
        transport: str = "auto",
        **kwargs: Any,
    ) -> Any:
        """
        Async RPC — runs the blocking ``PeerLink.call`` in a worker thread.

        Pass ``transport="tcp"`` for large payloads; ``"auto"`` (default)
        picks the right protocol automatically.
        """
        return await asyncio.to_thread(
            self._sync.call,
            target, func_name, *args,
            timeout=timeout, transport=transport, **kwargs,
        )

    def peer(self, name: str) -> AsyncPeerProxy:
        """
        Return an :class:`AsyncPeerProxy` for *name*.

        Raises :exc:`~peerlink.exceptions.PeerNotFound` immediately if the
        peer is not in the discovery cache.
        """
        if self._sync._discovery and self._sync._discovery.resolve(name) is None:
            raise PeerNotFound(f"Peer '{name}' not found")
        return AsyncPeerProxy(self, name)

    def peer_names(self) -> list[str]:
        """Sync read of cached peer names (no I/O)."""
        return self._sync.peer_names()
