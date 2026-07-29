"""
Core PeerLink P2P RPC implementation.

Zero-config P2P RPC over LAN/WiFi using:
  - mDNS discovery (zeroconf)
  - UDP transport
  - JSON message framing
"""

from __future__ import annotations

import contextvars
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import logging
import queue
import socket
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, List, Optional, Tuple, TypeVar

from zeroconf import ServiceBrowser, ServiceInfo, ServiceListener, Zeroconf

__all__ = [
    "CallResult",
    "Channel",
    "DatagramStream",
    "MAX_SAFE_UDP_PAYLOAD",
    "PeerLink",
    "current_peer",
    "run_with_current_peer",
    "SwarmNode",
    "PeerProxy",
    "PeerLinkError",
    "PeerNotFound",
    "PeerNotFoundError",
    "PeerTimeoutError",
    "RemoteError",
    "arp_scan",
    "__version__",
]

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
__version__ = "1.1.1"

# mDNS service type for PeerLink nodes
SERVICE_TYPE = "_peerlink._tcp.local."

# UDP transport configuration
BASE_PORT = 49152  # IANA dynamic/ephemeral range start
PORT_RANGE = 16383  # 49152 – 65535
RPC_TIMEOUT = 5.0  # seconds
MAX_DATAGRAM = 65535  # absolute upper bound for UDP payload
# Safe single-datagram payload size to avoid IP fragmentation (typical MTU 1500,
# minus IP/UDP headers and JSON overhead). Payloads larger than this are
# rejected; do not raise MAX_DATAGRAM to "make it work"—fragmented UDP is
# often dropped on real LAN gear.
MAX_SAFE_UDP_PAYLOAD = 1200
PEER_TTL = 60.0  # seconds before a peer entry expires

# Channel ordering: RAW = pass-through; SEQUENCE = monotonic seq, drop stale/duplicate
CHANNEL_ORDERING_RAW = "RAW"
CHANNEL_ORDERING_SEQUENCE = "SEQUENCE"
QUEUE_POLICY_DROP_OLDEST = "drop_oldest"
QUEUE_POLICY_BLOCK = "block"

# Sentinel placed in channel queue on close so blocked recv() wakes immediately
_CHANNEL_CLOSED = object()
DISCOVERY_WAIT = 3.0  # seconds to let mDNS settle on start

logger = logging.getLogger("peerlink")

# Caller identity during RPC handler execution (request "src" node name).
# ContextVar is async-safe but does not cross ThreadPoolExecutor boundaries
# automatically—PeerLink submits RPC dispatch via copy_context().run so the
# handler runs in an explicit context. If your handler submits work to another
# executor, use run_with_current_peer() or copy_context().run when submitting.
current_peer: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "current_peer", default=None
)

# Bounded pool for RPC dispatch so we don't spawn unbounded threads; work is
# submitted with copy_context().run to preserve ContextVar visibility for the
# whole _handle_message → _dispatch_rpc chain.
RPC_EXECUTOR_MAX_WORKERS = 16


def run_with_current_peer(
    peer: Optional[str], fn: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    """
    Run ``fn`` in a context where ``current_peer.get()`` returns ``peer``.
    Use when submitting work to a ThreadPoolExecutor (or any thread) from
    inside an RPC handler so the worker still sees caller identity::

        executor.submit(
            contextvars.copy_context().run,
            lambda: run_with_current_peer(current_peer.get(), heavy, arg),
        )
    """
    token = current_peer.set(peer)
    try:
        return fn(*args, **kwargs)
    finally:
        current_peer.reset(token)


# ─────────────────────────────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────────────────────────────
class PeerLinkError(Exception):
    """Base class for PeerLink-specific errors."""


class PeerNotFound(PeerLinkError):
    """Raised when the target peer is not in the discovery table or has expired."""


class RemoteError(PeerLinkError):
    """Raised when the remote node returns an error payload."""


class PeerNotFoundError(PeerNotFound):
    """Backward-compatible alias for older name used in docs/demos."""


class PeerTimeoutError(TimeoutError, PeerLinkError):
    """Raised when an RPC call does not receive a reply within the timeout."""


T_co = TypeVar("T_co", covariant=True)


@dataclass
class CallResult(Generic[T_co]):
    """
    Typed wrapper for per-peer broadcast results. ``call(\"ALL\", ...)`` stores
    exception instances in the dict; this type makes success/failure explicit.
    """

    value: Optional[T_co]
    error: Optional[BaseException]
    success: bool

    @staticmethod
    def from_value_or_exc(val: Any) -> "CallResult[Any]":
        if isinstance(val, BaseException):
            return CallResult(value=None, error=val, success=False)
        return CallResult(value=val, error=None, success=True)


@dataclass
class PeerInfo:
    name: str
    addr: str
    port: int
    last_seen: float
    ttl: float = PEER_TTL
    # Stable per-process identity from mDNS TXT; distinguishes restarts/collisions
    instance_id: Optional[str] = None
    # Optional TCP port for streaming / file transfer (advertised via mDNS TXT).
    tcp_port: Optional[int] = None


@dataclass
class PendingCall:
    id: str
    event: threading.Event
    result: Optional[dict] = None


# ─────────────────────────────────────────────────────────────────────────────
# Streaming channels (bidirectional, persistent over UDP)
# ─────────────────────────────────────────────────────────────────────────────
# Message types on wire:
#   channel_open  { type, channel_id, name, src }
#   channel_ack   { type, channel_id }
#   channel_data  { type, channel_id, payload }
#   channel_close { type, channel_id }


def _reject_if_payload_too_large(data: bytes, context: str) -> None:
    if len(data) > MAX_SAFE_UDP_PAYLOAD:
        raise PeerLinkError(
            f"{context}: encoded size {len(data)} bytes exceeds "
            f"MAX_SAFE_UDP_PAYLOAD ({MAX_SAFE_UDP_PAYLOAD}) to avoid IP fragmentation"
        )


class _ChannelQueue:
    """
    Bounded or unbounded queue for channel recv. drop_oldest evicts one item
    when full under a single lock (no get_nowait/put_nowait race). close()
    wakes blocked recv() with PeerLinkError instead of waiting for timeout.
    """

    def __init__(
        self,
        maxsize: int,
        policy: str = QUEUE_POLICY_DROP_OLDEST,
    ) -> None:
        self._policy = policy
        self._lock = threading.Lock()
        self._closed = False
        self._bounded = maxsize > 0
        self._maxsize = maxsize if maxsize > 0 else 0
        # Always deque + Condition so close() can notify_all waiters.
        self._deque: deque[Any] = deque()
        self._not_empty = threading.Condition(self._lock)

    def close(self) -> None:
        """Wake any recv() waiters; subsequent get() raises PeerLinkError."""
        with self._lock:
            self._closed = True
            self._deque.append(_CHANNEL_CLOSED)
            self._not_empty.notify_all()

    def put(self, item: Any) -> None:
        with self._lock:
            if self._closed:
                return
            if self._bounded and len(self._deque) >= self._maxsize:
                if self._policy == QUEUE_POLICY_DROP_OLDEST:
                    # Drop oldest under same lock (no get/put race)
                    if self._deque and self._deque[0] is not _CHANNEL_CLOSED:
                        self._deque.popleft()
                else:
                    while (
                        self._bounded
                        and len(self._deque) >= self._maxsize
                    ):
                        self._not_empty.wait(timeout=0.05)
                    if (
                        self._bounded
                        and len(self._deque) >= self._maxsize
                        and self._deque
                        and self._deque[0] is not _CHANNEL_CLOSED
                    ):
                        self._deque.popleft()
            self._deque.append(item)
            self._not_empty.notify()

    def get(self, timeout: Optional[float] = None) -> Any:
        with self._lock:
            if self._closed and not self._deque:
                raise PeerLinkError("Channel is closed")
            if timeout is None:
                while not self._deque:
                    if self._closed:
                        raise PeerLinkError("Channel is closed")
                    self._not_empty.wait()
            else:
                end = time.time() + timeout
                while not self._deque:
                    remaining = end - time.time()
                    if remaining <= 0:
                        raise queue.Empty
                    self._not_empty.wait(timeout=min(remaining, 0.1))
                if not self._deque:
                    raise queue.Empty
            item = self._deque.popleft()
            if item is _CHANNEL_CLOSED:
                raise PeerLinkError("Channel is closed")
            return item


class Channel:
    """
    **Datagram channel** (not a TCP stream). Each ``send()`` is one UDP
    datagram unless ``ordering=SEQUENCE`` is set, in which case PeerLink adds
    monotonic ``seq`` and recv drops stale/duplicate frames (unreliable
    sequenced semantics for game state).

    Payloads must stay under ``MAX_SAFE_UDP_PAYLOAD`` (1200 bytes encoded).
    """

    def __init__(
        self,
        node: "PeerLink",
        channel_id: str,
        peer_name: str,
        remote_addr: Tuple[str, int],
        *,
        closed_event: Optional[threading.Event] = None,
        ordering: str = CHANNEL_ORDERING_RAW,
    ) -> None:
        self._node = node
        self._channel_id = channel_id
        self._peer_name = peer_name
        self._remote_addr = remote_addr
        self._ordering = ordering
        self._closed = closed_event or threading.Event()
        self._send_lock = threading.Lock()
        self._on_disconnect: Optional[Callable[[str], None]] = None

    @property
    def channel_id(self) -> str:
        return self._channel_id

    @property
    def peer_name(self) -> str:
        return self._peer_name

    def set_on_disconnect(self, cb: Optional[Callable[[str], None]]) -> None:
        """Callback invoked with reason when channel ends (local close, peer close, etc.)."""
        self._on_disconnect = cb
        with self._node._lock:
            self._node._channel_disconnect_callbacks[self._channel_id] = cb

    def send(self, payload: Any) -> None:
        """Send one frame on the channel (fire-and-forget over UDP)."""
        if self._closed.is_set():
            raise PeerLinkError("Channel is closed")
        msg: Dict[str, Any] = {
            "type": "channel_data",
            "channel_id": self._channel_id,
            "payload": payload,
        }
        if self._ordering == CHANNEL_ORDERING_SEQUENCE:
            with self._node._lock:
                seq = self._node._channel_seq_out.get(self._channel_id, 0) + 1
                self._node._channel_seq_out[self._channel_id] = seq
            msg["seq"] = seq
        data = json.dumps(msg).encode("utf-8")
        if len(data) > MAX_DATAGRAM:
            raise PeerLinkError(f"Channel payload too large ({len(data)} bytes)")
        _reject_if_payload_too_large(data, "Channel.send")
        with self._send_lock:
            if self._node._sock is None:
                raise PeerLinkError("Node socket not available")
            self._node._sock.sendto(data, self._remote_addr)

    def recv(self, timeout: Optional[float] = None) -> Any:
        """Block until one payload is received on this channel."""
        if self._node._is_channel_closed(self._channel_id):
            raise PeerLinkError("Channel is closed")
        q = self._node._channel_queue(self._channel_id)
        try:
            return q.get(timeout=timeout)
        except queue.Empty:
            if self._node._is_channel_closed(self._channel_id):
                raise PeerLinkError("Channel is closed")
            raise PeerTimeoutError("Channel recv timed out")

    def close(self) -> None:
        """Signal close and send channel_close to peer."""
        if self._closed.is_set():
            return
        self._closed.set()
        self._node._unregister_channel_local(self._channel_id, reason="local_close")
        try:
            msg = {
                "type": "channel_close",
                "channel_id": self._channel_id,
                "reason": "closed",
            }
            data = json.dumps(msg).encode("utf-8")
            if self._node._sock and len(data) <= MAX_DATAGRAM:
                with self._send_lock:
                    self._node._sock.sendto(data, self._remote_addr)
        except OSError:
            pass

    def __enter__(self) -> "Channel":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


# Alias: name reflects datagram semantics (unordered/unreliable), not TCP stream
DatagramStream = Channel


class PeerProxy:
    """Lightweight proxy for attribute-based remote calls: peer.add(1, 2)."""

    def __init__(self, node: "PeerLink", name: str):
        self._node = node
        self._name = name

    def __repr__(self) -> str:
        return f"<PeerProxy name={self._name!r}>"

    def __getattr__(self, func_name: str) -> Callable[..., Any]:
        # Return a callable that forwards to PeerLink.call
        def _caller(*args, timeout: float = RPC_TIMEOUT, **kwargs: Any) -> Any:
            return self._node.call(self._name, func_name, *args, timeout=timeout, **kwargs)

        return _caller

    def is_alive(self, timeout: float = 2.0) -> bool:
        """Return True if the peer responds to a ping within the timeout."""
        try:
            res = self._node.call(self._name, "__ping__", timeout=timeout)
        except (PeerTimeoutError, PeerNotFound, RemoteError, OSError):
            return False
        if isinstance(res, dict):
            return bool(res.get("ok"))
        return bool(res)


class Stream:
    """
    TCP-backed stream used for long-lived byte streams (video, audio, files).

    Provides basic frame-oriented helpers on top of a blocking socket using
    a 4-byte big-endian length prefix for each frame.
    """

    def __init__(self, node: "PeerLink", sock: socket.socket, peer_name: str):
        self._node = node
        self._sock = sock
        self._peer_name = peer_name
        self._lock = threading.Lock()

    @property
    def peer_name(self) -> str:
        return self._peer_name

    def write(self, data: bytes) -> None:
        with self._lock:
            self._sock.sendall(data)

    def read(self, n: int) -> bytes:
        return self._sock.recv(n)

    def write_frame(self, payload: bytes) -> None:
        if not isinstance(payload, (bytes, bytearray)):
            raise TypeError("Stream.write_frame expects bytes")
        length = len(payload).to_bytes(4, "big")
        with self._lock:
            self._sock.sendall(length + payload)

    def read_frame(self) -> bytes:
        header = self._recv_exact(4)
        if not header:
            return b""
        length = int.from_bytes(header, "big")
        if length <= 0:
            return b""
        return self._recv_exact(length)

    def _recv_exact(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                break
            buf += chunk
        return buf

    def close(self) -> None:
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _node_port(node_name: str, realm: Optional[str] = None) -> int:
    """Deterministic, collision-resistant port from node name (and optional realm)."""
    key = node_name if not realm else f"{realm}|{node_name}"
    digest = int(hashlib.md5(key.encode()).hexdigest(), 16)
    return BASE_PORT + (digest % PORT_RANGE)


def _node_tcp_port(node_name: str, realm: Optional[str] = None) -> int:
    """
    Deterministic TCP port for streaming / file transfer.

    Uses a separate hash salt so UDP / TCP ports are stable but distinct, and
    still stay within the dynamic range.
    """
    key = (node_name if not realm else f"{realm}|{node_name}") + "|tcp"
    digest = int(hashlib.md5(key.encode()).hexdigest(), 16)
    return BASE_PORT + (digest % PORT_RANGE)


def _derive_realm(secret: Optional[str]) -> Optional[str]:
    if not secret:
        return None
    return hashlib.sha256(f"peerlink|{secret}".encode()).hexdigest()[:16]


def _local_ip() -> str:
    """Best-guess LAN IP (avoids 127.x loopback)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# ─────────────────────────────────────────────────────────────────────────────
# ARP Fallback (no Zeroconf env)
# ─────────────────────────────────────────────────────────────────────────────
def arp_scan(port: int, timeout: float = 2.0) -> List[Tuple[str, int]]:
    """
    Simple ARP-like UDP probe: scans 192.168.x.0/24 for PeerLink nodes.
    Returns list of (ip, port) tuples that respond.
    """
    my_ip = _local_ip()
    prefix = ".".join(my_ip.split(".")[:3])
    results: List[Tuple[str, int]] = []
    lock = threading.Lock()

    def probe(ip: str) -> None:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(timeout)
            ping = json.dumps({"id": "arp-ping", "rpc": "__ping__", "args": []}).encode()
            s.sendto(ping, (ip, port))
            data, _ = s.recvfrom(512)
            msg = json.loads(data)
            if msg.get("result") == "pong":
                with lock:
                    results.append((ip, port))
            s.close()
        except Exception:
            pass

    threads = [
        threading.Thread(target=probe, args=(f"{prefix}.{i}",), daemon=True)
        for i in range(1, 255)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout + 0.5)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# mDNS Peer Listener (callback bridge)
# ─────────────────────────────────────────────────────────────────────────────
class _PeerListener(ServiceListener):
    def __init__(self, node: "PeerLink"):
        self._node = node

    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zc.get_service_info(type_, name)
        if info:
            self._node._on_peer_added(name, info)

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        self._node._on_peer_removed(name)

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zc.get_service_info(type_, name)
        if info:
            self._node._on_peer_added(name, info)


# ─────────────────────────────────────────────────────────────────────────────
# Core PeerLink Node
# ─────────────────────────────────────────────────────────────────────────────
class PeerLink:
    """
    Zero-config P2P RPC node.

    Phase 1  Bootstrap  – UDP socket + mDNS publish
    Phase 2  Discovery  – ServiceBrowser auto-populates peers
    Phase 3  RPC call   – UDP fire → await reply event
    Phase 4  Swarm      – call("ALL", ...) fans out in parallel
    """

    def __init__(
        self,
        node_name: str,
        verbose: bool = True,
        *,
        secret: Optional[str] = None,
    ):
        self.node_name = node_name
        self.verbose = verbose
        self._secret = secret
        self._realm = _derive_realm(secret)
        # UDP port for RPC and datagram channels
        self.port = _node_port(node_name, self._realm)
        # TCP port for streaming and file transfer
        self._tcp_port = _node_tcp_port(node_name, self._realm)
        # One UUID per PeerLink process; published in mDNS TXT for collision
        # resolution (same display name, different instance).
        self._instance_id = str(uuid.uuid4())

        # Local RPC handlers and peer cache
        self._handlers: Dict[str, Callable[..., Any]] = {}
        self._peers: Dict[str, PeerInfo] = {}  # peer_name → PeerInfo
        self._pending: Dict[str, PendingCall] = {}  # call_id → PendingCall

        # Lifecycle hooks: invoked synchronously from discovery/prune paths
        # for instant notice (zeroconf thread or listener thread). Exceptions
        # in user callbacks are logged and never propagate.
        self._on_peer_up: Optional[
            Callable[[str, str, int], None]
        ] = None  # (name, addr, port) — set via set_peer_lifecycle
        self._on_peer_down: Optional[Callable[[str, str], None]] = None  # (name, reason)

        # Streaming channels: name -> accept callback; name -> ordering for open handshake
        self._channel_accept_handlers: Dict[str, Callable[["Channel"], None]] = {}
        self._channel_ordering_by_name: Dict[str, str] = {}
        self._channel_queues: Dict[str, _ChannelQueue] = {}
        self._channel_remote_addrs: Dict[str, Tuple[str, int]] = {}
        self._channel_ack_events: Dict[str, threading.Event] = {}
        self._channel_ordering: Dict[str, str] = {}  # channel_id -> RAW|SEQUENCE
        self._channel_last_seq_in: Dict[str, int] = {}
        self._channel_seq_out: Dict[str, int] = {}
        self._channel_closed: set[str] = set()
        self._channel_disconnect_callbacks: Dict[str, Callable[[str], None]] = {}

        self._lock = threading.Lock()
        self._running = False

        # Networking handles (set on start)
        self._sock: Optional[socket.socket] = None
        self._server_thread: Optional[threading.Thread] = None
        self._rpc_executor: Optional[ThreadPoolExecutor] = None
        self._zeroconf: Optional[Zeroconf] = None
        self._service_info: Optional[ServiceInfo] = None
        self._browser: Optional[ServiceBrowser] = None

        # TCP streaming / file-transfer server
        self._tcp_sock: Optional[socket.socket] = None
        self._tcp_server_thread: Optional[threading.Thread] = None
        self._stream_handlers: Dict[str, Callable[["Stream"], None]] = {}
        # Registered file handlers: list of (prefix, root_dir)
        self._file_roots: List[Tuple[str, str]] = []

        # Built-in ping (liveness + ARP fallback probe target)
        self._handlers["__ping__"] = lambda: {"ok": True, "time": time.time()}

        # Configure logging if verbose
        if self.verbose:
            if not logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter(
                    "[%(asctime)s][peerlink][%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S",
                )
                handler.setFormatter(formatter)
                logger.addHandler(handler)
            logger.setLevel(logging.INFO)

    # ── Public API ────────────────────────────────────────────────────

    def set_peer_lifecycle(
        self,
        on_up: Optional[Callable[[str, str, int], None]] = None,
        on_down: Optional[Callable[[str, str], None]] = None,
    ) -> "PeerLink":
        """
        Register callbacks for peer discovery lifecycle (chainable).

        **on_up(name, addr, port)** — called when a peer appears or **reconnects**
        with a new address/port (after optional on_down for the old endpoint).
        Invoked from the mDNS/browser thread immediately after cache update.

        **on_down(name, reason)** — called when a peer disappears.
        ``reason`` is one of:
          - ``"removed"`` — mDNS service removed
          - ``"expired"`` — TTL elapsed without refresh
          - ``"replaced"`` — same name, new address/port (followed by on_up)

        Callbacks must not block for long; spawn a thread if needed.
        """
        if on_up is not None:
            self._on_peer_up = on_up
        if on_down is not None:
            self._on_peer_down = on_down
        return self

    def _lifecycle_up(self, name: str, addr: str, port: int) -> None:
        cb = self._on_peer_up
        if not cb:
            return
        try:
            cb(name, addr, port)
        except Exception:
            logger.exception("on_peer_up callback failed for %s", name)

    def _lifecycle_down(self, name: str, reason: str) -> None:
        cb = self._on_peer_down
        if not cb:
            return
        try:
            cb(name, reason)
        except Exception:
            logger.exception("on_peer_down callback failed for %s", name)

    def register(self, name: str, func: Callable[..., Any]) -> "PeerLink":
        """
        Register an RPC function. Chainable.

        While the handler runs, ``current_peer.get()`` returns the caller's
        node name (the request ``src`` field). Use this for auth or per-peer
        state without changing every handler signature.
        """
        self._handlers[name] = func
        return self

    def start(self) -> "PeerLink":
        """
        Bootstrap sequence:
          1. Bind UDP socket (SOCK_REUSEADDR)
          2. Publish mDNS service record
          3. Launch ServiceBrowser for peer discovery
        """
        self._start_udp_server()
        self._start_tcp_server()
        self._publish_mdns()
        self._start_discovery()
        self._log(f"🚀 Node-{self.node_name} [udp={self.port} tcp={self._tcp_port}] online")
        return self

    def stop(self) -> None:
        """Graceful teardown: unregister mDNS, close UDP socket."""
        self._running = False
        if self._rpc_executor is not None:
            try:
                self._rpc_executor.shutdown(wait=True, cancel_futures=False)
            except Exception:
                pass
            self._rpc_executor = None
        try:
            if self._browser:
                self._browser.cancel()
            if self._zeroconf and self._service_info:
                self._zeroconf.unregister_service(self._service_info)
                self._zeroconf.close()
            if self._sock:
                self._sock.close()
            if self._tcp_sock:
                self._tcp_sock.close()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Shutdown error: %s", exc)
        self._log(f"🛑 Node-{self.node_name} stopped")

    def call(
        self,
        target: str,
        func_name: str,
        *args: Any,
        timeout: float = RPC_TIMEOUT,
        **kwargs: Any,
    ) -> Any:
        """
        Make an RPC call.

        target=\"ALL\"   → fan-out broadcast, returns {peer_name: result}
        target=\"Phone1\" → unicast, returns single result value
        """
        if target.strip().upper() == "ALL":
            return self._call_all(func_name, *args, timeout=timeout, **kwargs)
        return self._call_one(target, func_name, *args, timeout=timeout, **kwargs)

    def wait_for_peers(self, count: int = 1, timeout: float = 30.0) -> bool:
        """Block until at least `count` peers are discovered (or timeout)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if len(self._peers) >= count:
                    return True
            time.sleep(0.2)
        return False

    def peer_names(self) -> List[str]:
        """Return human-readable list of discovered peer names."""
        self._prune_peers()
        with self._lock:
            return sorted(self._peers.keys())

    def register_channel(
        self,
        name: str,
        on_accept: Optional[Callable[["Channel"], None]] = None,
        *,
        ordering: str = CHANNEL_ORDERING_RAW,
    ) -> "PeerLink":
        """
        Register a named channel endpoint. ``ordering=SEQUENCE`` requires
        open_channel(..., ordering=SEQUENCE) on the initiator; stale/duplicate
        datagrams are dropped on recv.
        """
        self._channel_accept_handlers[name] = on_accept or (lambda _c: None)
        self._channel_ordering_by_name[name] = ordering
        return self

    def open_channel(
        self,
        peer_name: str,
        channel_name: str,
        *,
        timeout: float = 5.0,
        ordering: str = CHANNEL_ORDERING_RAW,
        max_queue_size: int = 0,
        queue_policy: str = QUEUE_POLICY_DROP_OLDEST,
        idle_timeout: Optional[float] = None,
    ) -> Channel:
        """
        Open a bidirectional channel. ``max_queue_size=0`` is unbounded;
        otherwise inbound queue drops oldest when full (default policy).
        """
        if not self._running or self._sock is None:
            raise PeerLinkError("Node is not started")
        peer_addr = self._resolve_peer(peer_name)
        if peer_addr is None:
            raise PeerNotFound(f"Peer '{peer_name}' not found for channel open")
        channel_id = str(uuid.uuid4())
        ack_event = threading.Event()
        with self._lock:
            self._channel_ack_events[channel_id] = ack_event
            self._channel_remote_addrs[channel_id] = peer_addr
            self._channel_queues[channel_id] = _ChannelQueue(
                max_queue_size, queue_policy
            )
            self._channel_ordering[channel_id] = ordering
            if ordering == CHANNEL_ORDERING_SEQUENCE:
                self._channel_seq_out[channel_id] = 0
                self._channel_last_seq_in[channel_id] = 0
        msg: Dict[str, Any] = {
            "type": "channel_open",
            "channel_id": channel_id,
            "name": channel_name,
            "src": self.node_name,
            "ordering": ordering,
            "max_queue_size": max_queue_size,
            "queue_policy": queue_policy,
        }
        if idle_timeout is not None:
            msg["idle_timeout"] = idle_timeout
        # Lets acceptor reset seq state if peer restarts with new session
        msg["instance_id"] = self._instance_id
        payload = json.dumps(msg).encode("utf-8")
        if len(payload) > MAX_DATAGRAM:
            self._unregister_channel_local(channel_id)
            raise PeerLinkError("channel_open payload too large")
        _reject_if_payload_too_large(payload, "open_channel")
        self._sock.sendto(payload, peer_addr)
        if not ack_event.wait(timeout=timeout):
            self._unregister_channel_local(channel_id)
            raise PeerTimeoutError(
                f"Channel open to '{peer_name}' timed out after {timeout}s"
            )
        with self._lock:
            self._channel_ack_events.pop(channel_id, None)
        return Channel(
            self, channel_id, peer_name, peer_addr, ordering=ordering
        )

    def _channel_queue(self, channel_id: str) -> _ChannelQueue:
        with self._lock:
            if channel_id not in self._channel_queues:
                self._channel_queues[channel_id] = _ChannelQueue(0)
            return self._channel_queues[channel_id]

    def _is_channel_closed(self, channel_id: str) -> bool:
        with self._lock:
            return channel_id in self._channel_closed

    def _ingest_channel_data(self, msg: dict, channel_id: str) -> None:
        """
        Ingest one channel_data datagram. Must run from the single server
        recv thread (or with equivalent serialization) so SEQUENCE last_seq
        updates are not racy.
        """
        payload = msg.get("payload")
        with self._lock:
            if channel_id in self._channel_closed:
                return
            ordering = self._channel_ordering.get(
                channel_id, CHANNEL_ORDERING_RAW
            )
            if ordering == CHANNEL_ORDERING_SEQUENCE:
                seq = msg.get("seq")
                if seq is None:
                    return
                try:
                    seq = int(seq)
                except (TypeError, ValueError):
                    return
                last = self._channel_last_seq_in.get(channel_id, 0)
                if seq <= last:
                    return
                self._channel_last_seq_in[channel_id] = seq
            q = self._channel_queues.get(channel_id)
            if q is None:
                # No open channel — drop (do not create orphan queue)
                return
        q.put(payload)

    def _unregister_channel_local(self, channel_id: str, reason: str = "closed") -> None:
        cb: Optional[Callable[[str], None]] = None
        q_to_close: Optional[_ChannelQueue] = None
        with self._lock:
            self._channel_closed.add(channel_id)
            q_to_close = self._channel_queues.pop(channel_id, None)
            self._channel_remote_addrs.pop(channel_id, None)
            self._channel_ack_events.pop(channel_id, None)
            self._channel_ordering.pop(channel_id, None)
            self._channel_last_seq_in.pop(channel_id, None)
            self._channel_seq_out.pop(channel_id, None)
            cb = self._channel_disconnect_callbacks.pop(channel_id, None)
        if q_to_close is not None:
            try:
                q_to_close.close()
            except Exception:
                pass
        if cb is not None:
            try:
                cb(reason)
            except Exception:
                logger.exception("on_disconnect callback failed")

    def _handle_channel_message(self, msg: dict, addr: Tuple[str, int]) -> None:
        mtype = msg.get("type")
        channel_id = msg.get("channel_id")
        if not channel_id:
            return
        if mtype == "channel_open":
            name = msg.get("name", "")
            src = msg.get("src", "")
            open_ordering = msg.get("ordering", CHANNEL_ORDERING_RAW)
            if open_ordering not in (CHANNEL_ORDERING_RAW, CHANNEL_ORDERING_SEQUENCE):
                open_ordering = CHANNEL_ORDERING_RAW
            registered_ordering = self._channel_ordering_by_name.get(
                name, CHANNEL_ORDERING_RAW
            )
            handler = self._channel_accept_handlers.get(name)
            if handler is None:
                try:
                    self._sock.sendto(
                        json.dumps(
                            {
                                "type": "channel_close",
                                "channel_id": channel_id,
                                "reason": "no_handler",
                            }
                        ).encode("utf-8"),
                        addr,
                    )
                except OSError:
                    pass
                return
            if registered_ordering != open_ordering:
                try:
                    self._sock.sendto(
                        json.dumps(
                            {
                                "type": "channel_close",
                                "channel_id": channel_id,
                                "reason": "ordering_mismatch",
                            }
                        ).encode("utf-8"),
                        addr,
                    )
                except OSError:
                    pass
                return
            max_q = int(msg.get("max_queue_size") or 0)
            policy = msg.get("queue_policy") or QUEUE_POLICY_DROP_OLDEST
            if policy not in (QUEUE_POLICY_DROP_OLDEST, QUEUE_POLICY_BLOCK):
                policy = QUEUE_POLICY_DROP_OLDEST
            with self._lock:
                self._channel_remote_addrs[channel_id] = addr
                self._channel_queues[channel_id] = _ChannelQueue(max_q, policy)
                self._channel_ordering[channel_id] = open_ordering
                if open_ordering == CHANNEL_ORDERING_SEQUENCE:
                    self._channel_seq_out[channel_id] = 0
                    # New channel_open always resets seq window (new channel_id
                    # after restart; instance_id in msg documents sender session)
                    self._channel_last_seq_in[channel_id] = 0
            ack = json.dumps({"type": "channel_ack", "channel_id": channel_id}).encode(
                "utf-8"
            )
            try:
                self._sock.sendto(ack, addr)
            except OSError:
                self._unregister_channel_local(channel_id, reason="send_failed")
                return
            ch = Channel(
                self, channel_id, src or "peer", addr, ordering=open_ordering
            )
            try:
                handler(ch)
            except Exception:
                logger.exception("channel on_accept handler failed")
                self._unregister_channel_local(channel_id, reason="handler_failed")
        elif mtype == "channel_ack":
            with self._lock:
                ev = self._channel_ack_events.get(channel_id)
            if ev:
                ev.set()
        elif mtype == "channel_data":
            # channel_data is ingested inline from _server_loop only so SEQUENCE
            # ordering is not racy across executor threads. If reached here
            # (e.g. tests), use same single-threaded ingest.
            self._ingest_channel_data(msg, channel_id)
        elif mtype == "channel_close":
            reason = msg.get("reason", "closed")
            if not isinstance(reason, str):
                reason = "closed"
            self._unregister_channel_local(channel_id, reason=reason)

    def peer(self, name: str) -> PeerProxy:
        """
        Return a PeerProxy for attribute-based calls: peer("Phone1").add(1, 2).

        Raises PeerNotFound if the peer is not currently known.
        """
        if self._resolve_peer(name) is None:
            raise PeerNotFound(f"Peer '{name}' not found")
        return PeerProxy(self, name)

    # ── Context manager ───────────────────────────────────────────────

    def __enter__(self) -> "PeerLink":
        return self.start()

    def __exit__(self, *_: object) -> None:
        self.stop()

    def __repr__(self) -> str:  # pragma: no cover - repr only
        return (
            f"<PeerLink node={self.node_name!r} "
            f"port={self.port} peers={len(self._peers)}>"
        )

    # ── Phase 1: UDP server ───────────────────────────────────────────

    def _start_udp_server(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self._sock.bind(("", self.port))
        except OSError as exc:
            raise PeerLinkError(
                f"Cannot bind UDP port {self.port} for node '{self.node_name}': {exc}"
            ) from exc
        self._sock.settimeout(1.0)
        self._running = True
        self._server_thread = threading.Thread(
            target=self._server_loop,
            daemon=True,
            name=f"peerlink-{self.node_name}",
        )
        self._server_thread.start()
        self._rpc_executor = ThreadPoolExecutor(
            max_workers=RPC_EXECUTOR_MAX_WORKERS,
            thread_name_prefix=f"peerlink-rpc-{self.node_name}",
        )
        self._log(f"UDP server bound on port {self.port}")

    def _start_tcp_server(self) -> None:
        """
        Start TCP listener for streaming and file transfer.

        If binding fails (e.g. port in use), log a warning and continue with
        UDP-only mode so existing behavior is preserved.
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("", self._tcp_port))
            sock.listen()
            sock.settimeout(1.0)
        except OSError as exc:
            logger.warning(
                "TCP server bind failed on port %s for node '%s': %s",
                self._tcp_port,
                self.node_name,
                exc,
            )
            self._tcp_sock = None
            return
        self._tcp_sock = sock
        self._tcp_server_thread = threading.Thread(
            target=self._tcp_server_loop,
            daemon=True,
            name=f"peerlink-tcp-{self.node_name}",
        )
        self._tcp_server_thread.start()
        self._log(f"TCP server bound on port {self._tcp_port}")

    def _server_loop(self) -> None:
        """Receive UDP packets in a tight loop."""
        while self._running:
            try:
                data, addr = self._sock.recvfrom(MAX_DATAGRAM)
            except socket.timeout:
                # periodic peer pruning while idle
                self._prune_peers()
                continue
            except OSError:
                break
            try:
                msg = json.loads(data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.warning("Malformed packet from %s", addr)
                continue
            # channel_data: ingest here (single thread) so SEQUENCE seq filter
            # is not racy; also avoids flooding ThreadPoolExecutor with frames.
            if msg.get("type") == "channel_data":
                cid = msg.get("channel_id")
                if cid:
                    self._ingest_channel_data(msg, cid)
                continue
            # Preserve ContextVar chain across executor boundary: run the whole
            # dispatch in a copied context so current_peer is consistent for
            # synchronous handler code. Handlers that submit to other executors
            # should use copy_context().run or run_with_current_peer when submitting.
            if self._running and self._rpc_executor is not None:
                try:
                    ctx = contextvars.copy_context()
                    self._rpc_executor.submit(ctx.run, self._handle_message, msg, addr)
                except RuntimeError:
                    # Executor already shut down during stop()
                    break
            elif not self._running:
                break
            else:
                threading.Thread(
                    target=self._handle_message,
                    args=(msg, addr),
                    daemon=True,
                ).start()

    def _handle_message(self, msg: dict, addr: Tuple[str, int]) -> None:
        msg_type = msg.get("type")

        # Streaming channel frames (bidirectional, not RPC)
        if msg_type in {
            "channel_open",
            "channel_ack",
            "channel_data",
            "channel_close",
        }:
            self._handle_channel_message(msg, addr)
            return

        # Backwards compatibility: if no explicit type but has "rpc", treat as request
        if msg_type == "request" or ("rpc" in msg and msg_type is None):
            self._dispatch_rpc(msg, addr)
        elif msg_type in {"response", "error"} or "result" in msg or "error" in msg:
            call_id = msg.get("id")
            with self._lock:
                pending = self._pending.get(call_id)
            if pending:
                pending.result = msg
                pending.event.set()

    def _dispatch_rpc(self, msg: dict, addr: Tuple[str, int]) -> None:
        call_id = msg.get("id")
        func_name = msg.get("rpc", "")
        args = msg.get("args", [])
        kwargs = msg.get("kwargs", {})

        if func_name in self._handlers:
            try:
                caller = msg.get("src")
                if caller is not None and not isinstance(caller, str):
                    caller = str(caller)
                # Run handler inside copy_context so current_peer is bound in an
                # explicit Context; nested sync code sees it. Executor boundary
                # from handler code must use run_with_current_peer / copy_context.
                def _invoke() -> Any:
                    return run_with_current_peer(
                        caller, self._handlers[func_name], *args, **kwargs
                    )

                result = contextvars.copy_context().run(_invoke)
                resp: Dict[str, Any] = {
                    "type": "response",
                    "id": call_id,
                    "result": result,
                }
            except Exception as exc:  # pragma: no cover - remote execution path
                resp = {
                    "type": "error",
                    "id": call_id,
                    "error": {
                        "type": type(exc).__name__,
                        "message": str(exc),
                    },
                }
        else:
            resp = {
                "type": "error",
                "id": call_id,
                "error": {
                    "type": "RemoteError",
                    "message": f"No function '{func_name}' registered",
                },
            }

        try:
            payload = json.dumps(resp).encode("utf-8")
            if len(payload) > MAX_SAFE_UDP_PAYLOAD:
                resp = {
                    "type": "error",
                    "id": call_id,
                    "error": {
                        "type": "PeerLinkError",
                        "message": (
                            f"RPC result too large ({len(payload)} bytes encoded); "
                            f"keep return value under MAX_SAFE_UDP_PAYLOAD "
                            f"({MAX_SAFE_UDP_PAYLOAD})"
                        ),
                    },
                }
                payload = json.dumps(resp).encode("utf-8")
            if len(payload) > MAX_DATAGRAM:
                logger.error("Response too large to send (size=%d bytes)", len(payload))
                return
            self._sock.sendto(payload, addr)
        except OSError as exc:  # pragma: no cover - network failure
            logger.warning("Reply to %s failed: %s", addr, exc)

    # ── Phase 2: mDNS Discovery ───────────────────────────────────────

    def _service_type(self) -> str:
        """mDNS service type; scoped by realm when secret is set."""
        if self._realm:
            return f"_peerlink-{self._realm}._tcp.local."
        return SERVICE_TYPE

    def _publish_mdns(self) -> None:
        local_ip = _local_ip()
        self._zeroconf = Zeroconf()
        st = self._service_type()
        self._service_info = ServiceInfo(
            type_=st,
            name=f"{self.node_name}.{st}",
            addresses=[socket.inet_aton(local_ip)],
            port=self.port,
            properties={
                "node": self.node_name,
                "port": str(self.port),
                "tcp_port": str(self._tcp_port),
                "version": __version__,
                "instance_id": self._instance_id,
            },
            server=f"{self.node_name}.local.",
        )
        self._zeroconf.register_service(self._service_info)

    def _start_discovery(self) -> None:
        listener = _PeerListener(self)
        self._browser = ServiceBrowser(self._zeroconf, self._service_type(), listener)

    # ── TCP Server: Streams + Files ──────────────────────────────────────────

    def _tcp_server_loop(self) -> None:
        if self._tcp_sock is None:
            return
        while self._running and self._tcp_sock is not None:
            try:
                conn, addr = self._tcp_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(
                target=self._handle_tcp_connection,
                args=(conn, addr),
                daemon=True,
            ).start()

    def _handle_tcp_connection(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        """
        Demultiplex TCP connections between stream handshakes and minimal HTTP
        (file transfer) based on the first bytes.
        """
        try:
            conn.settimeout(5.0)
            # Peek at first line
            data = b""
            while b"\n" not in data and len(data) < 4096:
                chunk = conn.recv(1)
                if not chunk:
                    break
                data += chunk
            if not data:
                conn.close()
                return
            first_line = data.splitlines()[0].strip()
            if first_line.startswith(b"GET ") or first_line.startswith(b"PUT "):
                self._handle_http_connection(conn, first_line, initial=data)
            else:
                self._handle_stream_connection(conn, first_line, initial=data)
        except Exception:
            conn.close()

    # ── Stream API over TCP ──────────────────────────────────────────────────

    def register_stream(
        self,
        name: str,
        on_accept: Callable[["Stream"], None],
    ) -> "PeerLink":
        """
        Register a named TCP stream endpoint.

        The handler receives a ``Stream`` object backed by a TCP socket. It may
        use ``read_frame`` / ``write_frame`` for length-prefixed framing.
        """
        self._stream_handlers[name] = on_accept
        return self

    def open_stream(
        self,
        peer_name: str,
        stream_name: str,
        *,
        timeout: float = 5.0,
    ) -> "Stream":
        """
        Open a TCP stream to ``peer_name`` and perform a simple JSON handshake.
        """
        peer_addr = self._resolve_peer(peer_name)
        if peer_addr is None:
            raise PeerNotFound(f"Peer '{peer_name}' not found for stream open")
        addr, _ = peer_addr
        peer_info = self._peers.get(peer_name)
        tcp_port = None
        if peer_info is not None and peer_info.tcp_port is not None:
            tcp_port = peer_info.tcp_port
        else:
            tcp_port = _node_tcp_port(peer_name, self._realm)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((addr, tcp_port))
        # Handshake: JSON line
        hello = {
            "type": "stream_open",
            "name": stream_name,
            "src": self.node_name,
        }
        s.sendall(json.dumps(hello).encode("utf-8") + b"\n")
        # Optional ACK (ignored on timeout)
        try:
            ack = s.recv(1024)
            if not ack:
                raise PeerTimeoutError("Stream open did not receive ACK")
        except socket.timeout:
            raise PeerTimeoutError("Stream open did not receive ACK in time")
        return Stream(self, s, peer_name)

    def _handle_stream_connection(
        self,
        conn: socket.socket,
        first_line: bytes,
        initial: bytes,
    ) -> None:
        """
        Handle an incoming stream handshake and dispatch to the registered
        handler, if any.
        """
        try:
            # first_line already read; for now assume it is entire JSON
            try:
                hello = json.loads(first_line.decode("utf-8"))
            except Exception:
                conn.close()
                return
            if hello.get("type") != "stream_open":
                conn.close()
                return
            name = hello.get("name")
            src = hello.get("src") or "peer"
            handler = self._stream_handlers.get(name or "")
            if handler is None:
                conn.sendall(
                    json.dumps(
                        {
                            "type": "stream_error",
                            "error": f"No stream handler for '{name}'",
                        }
                    ).encode("utf-8")
                    + b"\n"
                )
                conn.close()
                return
            # ACK
            conn.sendall(
                json.dumps({"type": "stream_ack", "name": name}).encode("utf-8")
                + b"\n"
            )
            stream = Stream(self, conn, src)
            try:
                handler(stream)
            finally:
                stream.close()
        except Exception:
            conn.close()

    # ── File transfer over TCP (minimal HTTP/1.1 subset) ─────────────────────

    def register_file_handler(self, prefix: str, root_dir: str) -> "PeerLink":
        """
        Serve files under ``root_dir`` for HTTP requests whose path starts
        with ``prefix``. Path traversal outside ``root_dir`` is rejected.
        """
        self._file_roots.append((prefix.rstrip("/") or "/", root_dir))
        return self

    def get_file(
        self,
        peer_name: str,
        path: str,
        *,
        timeout: float = 10.0,
    ) -> bytes:
        peer_addr = self._resolve_peer(peer_name)
        if peer_addr is None:
            raise PeerNotFound(f"Peer '{peer_name}' not found for file GET")
        addr, _ = peer_addr
        peer_info = self._peers.get(peer_name)
        tcp_port = None
        if peer_info is not None and peer_info.tcp_port is not None:
            tcp_port = peer_info.tcp_port
        else:
            tcp_port = _node_tcp_port(peer_name, self._realm)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((addr, tcp_port))
        try:
            req = f"GET {path} HTTP/1.1\r\nHost: {peer_name}\r\n\r\n".encode("utf-8")
            s.sendall(req)
            header = b""
            while b"\r\n\r\n" not in header and len(header) < 65536:
                chunk = s.recv(1024)
                if not chunk:
                    break
                header += chunk
            if not header:
                raise RemoteError("Empty HTTP response")
            head, _, rest = header.partition(b"\r\n\r\n")
            status_line = head.splitlines()[0].decode("utf-8", errors="replace")
            if "200" not in status_line:
                raise RemoteError(f"HTTP error from peer: {status_line}")
            content_length = 0
            for line in head.splitlines()[1:]:
                try:
                    text = line.decode("utf-8")
                except Exception:
                    continue
                if text.lower().startswith("content-length:"):
                    try:
                        content_length = int(text.split(":", 1)[1].strip())
                    except ValueError:
                        pass
            body = rest
            while len(body) < content_length:
                chunk = s.recv(min(4096, content_length - len(body)))
                if not chunk:
                    break
                body += chunk
            return body
        finally:
            s.close()

    def put_file(
        self,
        peer_name: str,
        path: str,
        data: bytes,
        *,
        timeout: float = 10.0,
    ) -> None:
        peer_addr = self._resolve_peer(peer_name)
        if peer_addr is None:
            raise PeerNotFound(f"Peer '{peer_name}' not found for file PUT")
        addr, _ = peer_addr
        peer_info = self._peers.get(peer_name)
        tcp_port = None
        if peer_info is not None and peer_info.tcp_port is not None:
            tcp_port = peer_info.tcp_port
        else:
            tcp_port = _node_tcp_port(peer_name, self._realm)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((addr, tcp_port))
        try:
            headers = (
                f"PUT {path} HTTP/1.1\r\n"
                f"Host: {peer_name}\r\n"
                f"Content-Length: {len(data)}\r\n"
                f"\r\n"
            ).encode("utf-8")
            s.sendall(headers + data)
            # Minimal response parsing
            resp = b""
            while b"\r\n\r\n" not in resp and len(resp) < 4096:
                chunk = s.recv(1024)
                if not chunk:
                    break
                resp += chunk
            if not resp:
                raise RemoteError("Empty HTTP response to PUT")
            status_line = resp.splitlines()[0].decode("utf-8", errors="replace")
            if "200" not in status_line:
                raise RemoteError(f"HTTP error from peer on PUT: {status_line}")
        finally:
            s.close()

    def _handle_http_connection(
        self,
        conn: socket.socket,
        first_line: bytes,
        initial: bytes,
    ) -> None:
        """
        Minimal HTTP/1.1 handler for GET / PUT with Content-Length.
        """
        try:
            request = initial
            # Ensure we have full headers
            while b"\r\n\r\n" not in request and len(request) < 65536:
                chunk = conn.recv(1024)
                if not chunk:
                    break
                request += chunk
            head, _, body = request.partition(b"\r\n\r\n")
            lines = head.splitlines()
            if not lines:
                conn.close()
                return
            try:
                req_line = lines[0].decode("utf-8")
            except Exception:
                conn.close()
                return
            parts = req_line.split()
            if len(parts) < 2:
                conn.close()
                return
            method, path = parts[0], parts[1]
            path = path or "/"
            content_length = 0
            for line in lines[1:]:
                try:
                    text = line.decode("utf-8")
                except Exception:
                    continue
                if text.lower().startswith("content-length:"):
                    try:
                        content_length = int(text.split(":", 1)[1].strip())
                    except ValueError:
                        pass
            # Read body if needed
            while len(body) < content_length:
                chunk = conn.recv(min(4096, content_length - len(body)))
                if not chunk:
                    break
                body += chunk
            if method == "GET":
                self._http_serve_file(conn, path)
            elif method == "PUT":
                self._http_receive_file(conn, path, body)
            else:
                conn.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    def _match_file_root(self, path: str) -> Optional[Tuple[str, str]]:
        for prefix, root in self._file_roots:
            if path.startswith(prefix):
                return prefix, root
        return None

    def _http_serve_file(self, conn: socket.socket, path: str) -> None:
        import os
        from pathlib import Path

        match = self._match_file_root(path)
        if match is None:
            conn.sendall(b"HTTP/1.1 404 Not Found\r\n\r\n")
            conn.close()
            return
        prefix, root = match
        rel = path[len(prefix) :].lstrip("/")
        fs_path = Path(root) / rel
        try:
            fs_path = fs_path.resolve()
            root_path = Path(root).resolve()
            if root_path not in fs_path.parents and fs_path != root_path:
                raise PermissionError("Path traversal outside root")
            if not fs_path.is_file():
                raise FileNotFoundError(str(fs_path))
            data = fs_path.read_bytes()
        except FileNotFoundError:
            conn.sendall(b"HTTP/1.1 404 Not Found\r\n\r\n")
            conn.close()
            return
        except PermissionError:
            conn.sendall(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            conn.close()
            return
        headers = (
            f"HTTP/1.1 200 OK\r\nContent-Length: {len(data)}\r\n\r\n"
        ).encode("utf-8")
        conn.sendall(headers + data)
        conn.close()

    def _http_receive_file(self, conn: socket.socket, path: str, body: bytes) -> None:
        from pathlib import Path

        match = self._match_file_root(path)
        if match is None:
            conn.sendall(b"HTTP/1.1 404 Not Found\r\n\r\n")
            conn.close()
            return
        prefix, root = match
        rel = path[len(prefix) :].lstrip("/")
        fs_path = Path(root) / rel
        try:
            fs_path = fs_path.resolve()
            root_path = Path(root).resolve()
            if root_path not in fs_path.parents and fs_path != root_path:
                raise PermissionError("Path traversal outside root")
            fs_path.parent.mkdir(parents=True, exist_ok=True)
            fs_path.write_bytes(body)
        except PermissionError:
            conn.sendall(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            conn.close()
            return
        headers = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
        conn.sendall(headers)
        conn.close()

    def _on_peer_added(self, mdns_key: str, info: ServiceInfo) -> None:
        # Skip self
        if self.node_name.lower() in mdns_key.lower():
            return
        props = info.decoded_properties or {}
        peer_name = props.get("node", mdns_key.split(".")[0])
        instance_id = props.get("instance_id")
        if isinstance(instance_id, bytes):
            instance_id = instance_id.decode("utf-8", errors="replace")
        addr = socket.inet_ntoa(info.addresses[0]) if info.addresses else None
        port = info.port
        # Optional TCP port for streaming / files. If absent, fall back to UDP port.
        raw_tcp_port = props.get("tcp_port")
        if isinstance(raw_tcp_port, bytes):
            try:
                raw_tcp_port = raw_tcp_port.decode("utf-8", errors="replace")
            except Exception:
                raw_tcp_port = None
        try:
            tcp_port: Optional[int] = int(raw_tcp_port) if raw_tcp_port else None
        except (TypeError, ValueError):
            tcp_port = None
        if tcp_port is None:
            tcp_port = port
        if not addr:
            return
        now = time.time()
        old: Optional[PeerInfo] = None
        with self._lock:
            old = self._peers.get(peer_name)
            # Same name, different instance_id => treat as replacement (crash/restart)
            if old and instance_id and old.instance_id and instance_id != old.instance_id:
                self._peers.pop(peer_name, None)
            elif old and (old.addr != addr or old.port != port):
                # Reconnection with new endpoint — notify down then replace
                self._peers.pop(peer_name, None)
            self._peers[peer_name] = PeerInfo(
                name=peer_name,
                addr=addr,
                port=port,
                last_seen=now,
                instance_id=instance_id if isinstance(instance_id, str) else None,
                tcp_port=tcp_port,
            )
        self._log(f"👥 Found peer: {peer_name} @ {addr}:{port}")
        if old and (old.addr != addr or old.port != port):
            self._lifecycle_down(peer_name, "replaced")
        self._lifecycle_up(peer_name, addr, port)

    def _on_peer_removed(self, mdns_key: str) -> None:
        # mdns_key is service name; resolve approximate peer_name
        name = mdns_key.split(".")[0]
        with self._lock:
            removed = self._peers.pop(name, None)
        if removed:
            self._log(f"💀 Lost peer: {removed.name}")
            self._lifecycle_down(removed.name, "removed")

    def _prune_peers(self) -> None:
        """Drop peers that have exceeded their TTL."""
        now = time.time()
        expired_names: List[str] = []
        with self._lock:
            expired = [
                name
                for name, info in self._peers.items()
                if now - info.last_seen > info.ttl
            ]
            for name in expired:
                self._peers.pop(name, None)
                expired_names.append(name)
                self._log(f"⌛ Peer expired: {name}")
        for name in expired_names:
            self._lifecycle_down(name, "expired")

    # ── Phase 3: RPC call engine ──────────────────────────────────────

    def _resolve_peer(self, target: str) -> Optional[Tuple[str, int]]:
        """Fuzzy-match target name → (ip, port)."""
        self._prune_peers()
        t = target.lower()
        with self._lock:
            for name, info in self._peers.items():
                if t in name.lower():
                    return info.addr, info.port
        return None

    def _call_one(
        self,
        target: str,
        func_name: str,
        *args: Any,
        timeout: float,
        **kwargs: Any,
    ) -> Any:
        peer_addr = self._resolve_peer(target)
        if peer_addr is None:
            with self._lock:
                known = list(self._peers.keys())
            raise PeerNotFound(f"Peer '{target}' not found. Known peers: {known}")

        call_id = str(uuid.uuid4())
        event = threading.Event()
        pending = PendingCall(id=call_id, event=event)
        with self._lock:
            self._pending[call_id] = pending

        msg = {
            "type": "request",
            "id": call_id,
            "src": self.node_name,
            "dst": target,
            "rpc": func_name,
            "args": list(args),
            "kwargs": kwargs,
        }
        payload = json.dumps(msg).encode("utf-8")
        if len(payload) > MAX_DATAGRAM:
            with self._lock:
                self._pending.pop(call_id, None)
            raise PeerLinkError(
                f"Payload too large for UDP datagram ({len(payload)} bytes)"
            )
        _reject_if_payload_too_large(payload, "RPC request")

        self._log(f"RPC {func_name} → {target} (id={call_id})")
        self._sock.sendto(payload, peer_addr)

        fired = event.wait(timeout=timeout)
        with self._lock:
            reply = (
                self._pending.pop(call_id, None).result
                if call_id in self._pending
                else pending.result
            )

        if not fired or reply is None:
            self._log(f"RPC {func_name} → {target} (id={call_id}) timed out")
            raise PeerTimeoutError(
                f"RPC '{func_name}' → '{target}' timed out after {timeout}s"
            )

        if reply.get("type") == "error" or "error" in reply:
            err = reply.get("error")
            if isinstance(err, dict):
                raise RemoteError(f"{err.get('type')}: {err.get('message')}")
            raise RemoteError(str(err))

        return reply.get("result")

    # ── Phase 4: Swarm broadcast ──────────────────────────────────────

    def _call_all(
        self,
        func_name: str,
        *args: Any,
        timeout: float,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Fan-out RPC to every known peer in parallel threads."""
        self._prune_peers()
        with self._lock:
            snapshot = list(self._peers.items())

        results: Dict[str, Any] = {}
        lock = threading.Lock()

        def worker(peer_name: str, _: PeerInfo) -> None:
            try:
                res = self._call_one(
                    peer_name, func_name, *args, timeout=timeout, **kwargs
                )
                with lock:
                    results[peer_name] = res
            except Exception as exc:
                # Capture per-peer failures as exception instances (e.g. RemoteError,
                # PeerNotFound, PeerTimeoutError) instead of stopping the whole call.
                with lock:
                    results[peer_name] = exc

        threads = [
            threading.Thread(target=worker, args=(name, info), daemon=True)
            for name, info in snapshot
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout + 1)

        return results

    def call_all_results(
        self,
        func_name: str,
        *args: Any,
        timeout: float = RPC_TIMEOUT,
        **kwargs: Any,
    ) -> Dict[str, "CallResult[Any]"]:
        """
        Same fan-out as ``call(\"ALL\", ...)`` but returns ``CallResult`` per
        peer so callers avoid ``isinstance(x, Exception)`` on every value.
        """
        raw = self._call_all(func_name, *args, timeout=timeout, **kwargs)
        return {name: CallResult.from_value_or_exc(val) for name, val in raw.items()}

    # ── Logging ───────────────────────────────────────────────────────

    def _log(self, msg: str) -> None:
        if self.verbose:
            ts = time.strftime("%H:%M:%S")
            print(f"[{ts}][PeerLink:{self.node_name}] {msg}")


# ─────────────────────────────────────────────────────────────────────────────
# SwarmNode — High-level convenience wrapper
# ─────────────────────────────────────────────────────────────────────────────
class SwarmNode(PeerLink):
    """
    Auto-started PeerLink node. Designed for context-manager usage.

    Example:
        with SwarmNode("RPi4") as node:
            node.register("matrix_chunk", compute)
            node.wait_for_peers(2)
            results = node.call("ALL", "matrix_chunk", data)
    """

    def __init__(self, node_name: str, **kwargs: Any):
        super().__init__(node_name, **kwargs)
        self.start()

