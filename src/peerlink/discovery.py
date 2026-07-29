"""
peerlink.discovery
~~~~~~~~~~~~~~~~~~
mDNS peer discovery: publish this node, watch for others.

Responsibilities
----------------
- Publish a Zeroconf ``ServiceInfo`` so others can find us.
- Run a ``ServiceBrowser`` to maintain a live cache of peers.
- Prune peers that have not refreshed within their TTL.
- Fire ``on_up`` / ``on_down`` callbacks on peer state changes.

Thread-safety: all mutations to ``_peers`` happen under ``_lock``.
The public ``peers`` snapshot and ``resolve`` are safe to call from any thread.
"""

from __future__ import annotations

import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from zeroconf import ServiceBrowser, ServiceInfo, ServiceListener, Zeroconf

from .constants import PEER_TTL, SERVICE_TYPE
from ._utils import derive_realm, local_ip

__all__ = ["PeerInfo", "Discovery"]


@dataclass
class PeerInfo:
    name: str
    addr: str
    port: int           # UDP port
    tcp_port: int       # TCP port (same value when peer does not advertise separately)
    last_seen: float
    ttl: float = PEER_TTL
    instance_id: Optional[str] = None


class _Listener(ServiceListener):
    """Bridge Zeroconf callbacks into Discovery._on_added / _on_removed."""

    def __init__(self, disc: "Discovery") -> None:
        self._disc = disc

    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zc.get_service_info(type_, name)
        if info:
            self._disc._on_added(name, info)

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        self._disc._on_removed(name)

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zc.get_service_info(type_, name)
        if info:
            self._disc._on_added(name, info)


def _decode(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value) if value is not None else ""


class Discovery:
    """
    Owns the Zeroconf instance for a single PeerLink node.

    Usage::

        disc = Discovery("NodeA", udp_port=50000, tcp_port=50001)
        disc.set_callbacks(on_up=..., on_down=...)
        disc.start()
        # ... work ...
        disc.stop()
    """

    def __init__(
        self,
        node_name: str,
        udp_port: int,
        tcp_port: int,
        *,
        secret: Optional[str] = None,
    ) -> None:
        self.node_name = node_name
        self._udp_port = udp_port
        self._tcp_port = tcp_port
        self._realm = derive_realm(secret)
        self._instance_id = str(uuid.uuid4())

        self._peers: Dict[str, PeerInfo] = {}
        self._lock = threading.Lock()

        self._on_up: Optional[Callable[[str, str, int, int], None]] = None   # name,addr,udp,tcp
        self._on_down: Optional[Callable[[str, str], None]] = None           # name,reason

        self._zeroconf: Optional[Zeroconf] = None
        self._browser: Optional[ServiceBrowser] = None
        self._service_info: Optional[ServiceInfo] = None

    # ── Public ────────────────────────────────────────────────────────────────

    def set_callbacks(
        self,
        on_up: Optional[Callable[[str, str, int, int], None]] = None,
        on_down: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        if on_up is not None:
            self._on_up = on_up
        if on_down is not None:
            self._on_down = on_down

    def start(self) -> None:
        """Publish service record and start peer browser (blocking until registered)."""
        st = self._service_type()
        local = local_ip()
        props = {
            "node": self.node_name,
            "udp_port": str(self._udp_port),
            "tcp_port": str(self._tcp_port),
            "instance_id": self._instance_id,
        }
        self._service_info = ServiceInfo(
            type_=st,
            name=f"{self.node_name}.{st}",
            addresses=[socket.inet_aton(local)],
            port=self._udp_port,
            properties=props,
            server=f"{self.node_name}.local.",
        )
        self._zeroconf = Zeroconf()
        self._zeroconf.register_service(self._service_info)
        self._browser = ServiceBrowser(self._zeroconf, st, _Listener(self))

    def stop(self) -> None:
        """Unregister service and close Zeroconf."""
        if self._browser:
            self._browser.cancel()
        if self._zeroconf and self._service_info:
            try:
                self._zeroconf.unregister_service(self._service_info)
            except Exception:
                pass
        if self._zeroconf:
            try:
                self._zeroconf.close()
            except Exception:
                pass

    def peers(self) -> List[str]:
        """Snapshot of currently known peer names (after TTL prune)."""
        self._prune()
        with self._lock:
            return sorted(self._peers.keys())

    def resolve(self, target: str) -> Optional[PeerInfo]:
        """
        Case-insensitive substring match → ``PeerInfo``, or ``None``.

        Uses the cheapest match first: exact name, then substring.
        Always prunes stale entries before resolving.
        """
        self._prune()
        t = target.lower()
        with self._lock:
            # Exact match preferred
            if target in self._peers:
                return self._peers[target]
            for name, info in self._peers.items():
                if t in name.lower():
                    return info
        return None

    def wait_for_peers(self, count: int = 1, timeout: float = 30.0) -> bool:
        """Block until at least *count* peers are known, or *timeout* elapses."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            self._prune()
            with self._lock:
                if len(self._peers) >= count:
                    return True
            time.sleep(0.2)
        return False

    # ── Internal ──────────────────────────────────────────────────────────────

    def _service_type(self) -> str:
        if self._realm:
            return f"_peerlink-{self._realm}._tcp.local."
        return SERVICE_TYPE

    def _on_added(self, mdns_key: str, info: ServiceInfo) -> None:
        props = info.decoded_properties or {}
        name = _decode(props.get("node")) or mdns_key.split(".")[0]
        if not name or name == self.node_name:
            return

        addr = socket.inet_ntoa(info.addresses[0]) if info.addresses else None
        if not addr:
            return

        udp_port = info.port
        try:
            tcp_port = int(_decode(props.get("tcp_port")) or udp_port)
        except (ValueError, TypeError):
            tcp_port = udp_port

        instance_id = _decode(props.get("instance_id")) or None
        now = time.time()

        old_down_reason: Optional[str] = None
        with self._lock:
            old = self._peers.get(name)
            if old:
                if instance_id and old.instance_id and instance_id != old.instance_id:
                    old_down_reason = "replaced"
                elif old.addr != addr or old.port != udp_port:
                    old_down_reason = "replaced"
            self._peers[name] = PeerInfo(
                name=name,
                addr=addr,
                port=udp_port,
                tcp_port=tcp_port,
                last_seen=now,
                instance_id=instance_id,
            )

        if old_down_reason:
            self._fire_down(name, old_down_reason)
        self._fire_up(name, addr, udp_port, tcp_port)

    def _on_removed(self, mdns_key: str) -> None:
        name = mdns_key.split(".")[0]
        with self._lock:
            removed = self._peers.pop(name, None)
        if removed:
            self._fire_down(removed.name, "removed")

    def _prune(self) -> None:
        now = time.time()
        expired: List[str] = []
        with self._lock:
            for name, info in list(self._peers.items()):
                if now - info.last_seen > info.ttl:
                    del self._peers[name]
                    expired.append(name)
        for name in expired:
            self._fire_down(name, "expired")

    def _fire_up(self, name: str, addr: str, udp: int, tcp: int) -> None:
        cb = self._on_up
        if cb:
            try:
                cb(name, addr, udp, tcp)
            except Exception:
                pass

    def _fire_down(self, name: str, reason: str) -> None:
        cb = self._on_down
        if cb:
            try:
                cb(name, reason)
            except Exception:
                pass
