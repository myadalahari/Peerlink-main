"""
peerlink._utils
~~~~~~~~~~~~~~~
Pure helper functions: port derivation, IP detection, payload guard.
No side-effects on import; safe to import from every other module.
"""

from __future__ import annotations

import contextvars
import hashlib
import socket
from typing import Any, Callable, Optional

from .constants import BASE_PORT, MAX_SAFE_UDP_PAYLOAD, PORT_RANGE
from .exceptions import PayloadTooLarge

__all__ = [
    "derive_port",
    "derive_realm",
    "local_ip",
    "reject_if_too_large",
    "current_peer",
    "run_with_current_peer",
]

# ── Context variable carrying caller identity during RPC handler execution ──
# Populated by the dispatch path before invoking user handlers so handlers
# can call current_peer.get() without extra arguments.
current_peer: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "current_peer", default=None
)


def run_with_current_peer(
    peer: Optional[str], fn: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    """
    Invoke *fn* in a context where ``current_peer.get()`` returns *peer*.

    Use when handing off work to a ThreadPoolExecutor from inside an RPC
    handler so the worker still has access to caller identity::

        fut = executor.submit(
            contextvars.copy_context().run,
            run_with_current_peer, current_peer.get(), my_fn, arg
        )
    """
    token = current_peer.set(peer)
    try:
        return fn(*args, **kwargs)
    finally:
        current_peer.reset(token)


def derive_realm(secret: Optional[str]) -> Optional[str]:
    """Derive a short, opaque realm tag from a shared secret (or None)."""
    if not secret:
        return None
    return hashlib.sha256(f"peerlink|{secret}".encode()).hexdigest()[:16]


def derive_port(node_name: str, realm: Optional[str] = None, *, salt: str = "") -> int:
    """
    Deterministic, collision-resistant port from *node_name* (and optional realm).

    A *salt* suffix allows the same name to yield a different port for a
    different protocol (e.g., ``salt="tcp"``).
    """
    key = node_name if not realm else f"{realm}|{node_name}"
    if salt:
        key = f"{key}|{salt}"
    digest = int(hashlib.md5(key.encode()).hexdigest(), 16)
    return BASE_PORT + (digest % PORT_RANGE)


def local_ip() -> str:
    """Best-guess LAN IP; avoids 127.x loopback."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def reject_if_too_large(data: bytes, context: str = "payload") -> None:
    """
    Raise :exc:`PayloadTooLarge` when *data* exceeds the safe UDP size.

    Call this before every UDP ``sendto`` so the error is local and
    descriptive rather than silently truncated or fragmented by the OS.
    """
    if len(data) > MAX_SAFE_UDP_PAYLOAD:
        raise PayloadTooLarge(
            f"{context}: {len(data)} bytes > MAX_SAFE_UDP_PAYLOAD "
            f"({MAX_SAFE_UDP_PAYLOAD}).  Use TCP transport for large payloads."
        )
