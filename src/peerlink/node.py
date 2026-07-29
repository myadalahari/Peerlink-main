"""
peerlink.node
~~~~~~~~~~~~~
Core PeerLink node: discovery + RPC over UDP, large payloads over TCP.

Design goals for the MVP rewrite
---------------------------------
1. **Protocol is a first-class choice** — pass ``transport="udp"`` (default)
   or ``transport="tcp"`` to ``call()``.  UDP is used for small fast RPC;
   TCP is used automatically when the payload exceeds ``MAX_SAFE_UDP_PAYLOAD``.

2. **One class, clear phases** — ``PeerLink`` is the only public type.
   Helpers live in dedicated modules so this file stays readable.

3. **Explicit over magical** — no hidden threading tricks; each subsystem
   (discovery, udp transport, tcp transport, rpc dispatch) owns its thread(s).

4. **Fail loudly, fail early** — ``PayloadTooLarge`` tells you *why* and *what
   to do* instead of silently fragmenting UDP packets.

Public API
----------
.. code-block:: python

    node = PeerLink("NodeA")
    node.register("add", lambda a, b: a + b)
    with node:
        node.wait_for_peers(1)
        result = node.peer("NodeB").add(1, 2)       # UDP by default
        result = node.call("NodeB", "add", 1, 2, transport="tcp")  # force TCP
"""

from __future__ import annotations

import contextvars
import json
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from .constants import (
    DISCOVERY_WAIT,
    MAX_SAFE_UDP_PAYLOAD,
    RPC_EXECUTOR_MAX_WORKERS,
    RPC_TIMEOUT,
)
from .discovery import Discovery, PeerInfo
from .exceptions import (
    PeerLinkError,
    PeerNotFound,
    PeerTimeoutError,
    RemoteError,
)
from .transport import TCPTransport, UDPTransport, _FrameSocket
from ._utils import derive_port, derive_realm, local_ip, current_peer, run_with_current_peer

__all__ = ["PeerLink", "PeerProxy", "SwarmNode"]

logger = logging.getLogger("peerlink")

Addr = Tuple[str, int]


# ─────────────────────────────────────────────────────────────────────────────
# Internal: pending RPC call tracking
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _Pending:
    call_id: str
    event: threading.Event
    result: Optional[dict] = None


# ─────────────────────────────────────────────────────────────────────────────
# PeerProxy — attribute-style remote calls
# ─────────────────────────────────────────────────────────────────────────────

class PeerProxy:
    """
    Lightweight proxy for attribute-based remote calls.

    Usage::

        proxy = node.peer("NodeB")
        result = proxy.add(1, 2)
        result = proxy.add(1, 2, _transport="tcp")   # keyword overrides transport

    Any keyword argument prefixed with ``_`` is consumed locally and not
    forwarded to the remote handler.
    """

    def __init__(self, node: "PeerLink", name: str) -> None:
        self._node = node
        self._name = name

    def __repr__(self) -> str:
        return f"<PeerProxy name={self._name!r}>"

    def __getattr__(self, func_name: str) -> Callable[..., Any]:
        def _caller(
            *args: Any,
            _timeout: float = RPC_TIMEOUT,
            _transport: str = "auto",
            **kwargs: Any,
        ) -> Any:
            return self._node.call(
                self._name,
                func_name,
                *args,
                timeout=_timeout,
                transport=_transport,
                **kwargs,
            )
        return _caller

    def is_alive(self, timeout: float = 2.0) -> bool:
        """Return ``True`` if the peer responds to an internal ping."""
        try:
            res = self._node.call(self._name, "__ping__", timeout=timeout)
        except (PeerTimeoutError, PeerNotFound, RemoteError, OSError):
            return False
        return bool(res.get("ok")) if isinstance(res, dict) else bool(res)


# ─────────────────────────────────────────────────────────────────────────────
# PeerLink — main node
# ─────────────────────────────────────────────────────────────────────────────

class PeerLink:
    """
    Zero-config P2P RPC node.

    Parameters
    ----------
    node_name:
        Human-readable identity of this node on the LAN.
    verbose:
        Print timestamped log lines to stdout (in addition to the ``peerlink``
        logger).  Disable in production; use ``logging.getLogger("peerlink")``
        instead.
    secret:
        Optional shared secret.  Nodes with different secrets cannot see each
        other on the same LAN.

    Transports
    ----------
    ``call(..., transport="auto")`` (default)
        Chooses UDP for payloads < ``MAX_SAFE_UDP_PAYLOAD``, TCP otherwise.
    ``call(..., transport="udp")``
        Always use UDP; raises ``PayloadTooLarge`` if the payload is too big.
    ``call(..., transport="tcp")``
        Always use TCP; suitable for large payloads (files, blobs).

    Context manager::

        with PeerLink("NodeA") as node:
            node.register("add", lambda a, b: a + b)
            node.wait_for_peers(1)
            print(node.peer("NodeB").add(3, 4))
    """

    def __init__(
        self,
        node_name: str,
        verbose: bool = True,
        *,
        secret: Optional[str] = None,
    ) -> None:
        self.node_name = node_name
        self.verbose = verbose

        realm = derive_realm(secret)
        self._udp_port = derive_port(node_name, realm)
        self._tcp_port = derive_port(node_name, realm, salt="tcp")

        # RPC handlers; __ping__ is always registered
        self._handlers: Dict[str, Callable[..., Any]] = {
            "__ping__": lambda: {"ok": True, "ts": time.time()}
        }

        # Pending UDP calls: call_id → _Pending
        self._pending: Dict[str, _Pending] = {}
        self._lock = threading.Lock()
        self._running = False

        # Subsystems (created in start())
        self._discovery: Optional[Discovery] = None
        self._udp: Optional[UDPTransport] = None
        self._tcp: Optional[TCPTransport] = None
        self._executor: Optional[ThreadPoolExecutor] = None

        if verbose:
            self._configure_logger()

    # ── Public API ────────────────────────────────────────────────────────────

    def register(self, name: str, func: Callable[..., Any]) -> "PeerLink":
        """
        Register a callable as an RPC endpoint (chainable).

        While *func* executes, ``current_peer.get()`` returns the caller's
        node name so handlers can apply per-caller logic without changing
        their signature.
        """
        self._handlers[name] = func
        return self

    def start(self) -> "PeerLink":
        """
        Bootstrap:

        1. Bind UDP + TCP sockets.
        2. Publish mDNS service record.
        3. Start peer discovery browser.
        """
        if self._running:
            return self

        self._executor = ThreadPoolExecutor(
            max_workers=RPC_EXECUTOR_MAX_WORKERS,
            thread_name_prefix="peerlink-rpc",
        )

        # UDP transport
        self._udp = UDPTransport(self._udp_port, self._on_udp_message)
        self._udp.start()

        # TCP transport (receives large-payload RPC and streams)
        self._tcp = TCPTransport(self._tcp_port, self._on_tcp_connection)
        self._tcp.start()

        # Discovery
        self._discovery = Discovery(
            self.node_name,
            udp_port=self._udp_port,
            tcp_port=self._tcp_port,
        )
        self._discovery.start()

        self._running = True
        self._log(
            f"[START] {self.node_name}  udp={self._udp_port}  tcp={self._tcp_port}  online"
        )
        return self

    def stop(self) -> None:
        """Graceful teardown: unregister mDNS, close sockets, drain executor."""
        if not self._running:
            return
        self._running = False

        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None

        if self._discovery:
            self._discovery.stop()
        if self._udp:
            self._udp.stop()
        if self._tcp:
            self._tcp.stop()

        self._log(f"[STOP] {self.node_name} stopped")

    def call(
        self,
        target: str,
        func_name: str,
        *args: Any,
        timeout: float = RPC_TIMEOUT,
        transport: str = "auto",
        **kwargs: Any,
    ) -> Any:
        """
        Make an RPC call to *target*.

        Parameters
        ----------
        target:
            Peer name or ``"ALL"`` for fan-out broadcast.
        func_name:
            Name of the registered function on the remote side.
        timeout:
            Seconds to wait for a reply before raising ``PeerTimeoutError``.
        transport:
            ``"auto"`` (default), ``"udp"``, or ``"tcp"``.
            ``"auto"`` picks UDP for small payloads, TCP for large ones.

        Returns
        -------
        Single value for unicast; ``dict[peer_name, result_or_exception]``
        for broadcast (``target="ALL"``).
        """
        if target.strip().upper() == "ALL":
            return self._call_all(func_name, *args, timeout=timeout, transport=transport, **kwargs)
        return self._call_one(target, func_name, *args, timeout=timeout, transport=transport, **kwargs)

    def peer(self, name: str) -> PeerProxy:
        """
        Return a :class:`PeerProxy` for *name*.

        Raises ``PeerNotFound`` immediately if the peer is not in the cache
        (call ``wait_for_peers`` first if you need to block for arrival).
        """
        if not self._discovery or self._discovery.resolve(name) is None:
            with self._lock:
                known = self._discovery.peers() if self._discovery else []
            raise PeerNotFound(f"Peer '{name}' not found.  Known: {known}")
        return PeerProxy(self, name)

    def wait_for_peers(self, count: int = 1, timeout: float = 30.0) -> bool:
        """Block until at least *count* peers are discovered, or *timeout* elapses."""
        if not self._discovery:
            raise PeerLinkError("Node is not started")
        return self._discovery.wait_for_peers(count, timeout)

    def peer_names(self) -> List[str]:
        """Sorted list of currently known peer names."""
        if not self._discovery:
            return []
        return self._discovery.peers()

    def set_peer_lifecycle(
        self,
        on_up: Optional[Callable[[str, str, int, int], None]] = None,
        on_down: Optional[Callable[[str, str], None]] = None,
    ) -> "PeerLink":
        """
        Register lifecycle callbacks (chainable).

        ``on_up(name, addr, udp_port, tcp_port)`` — peer appeared or reconnected.
        ``on_down(name, reason)`` — peer gone; reason is ``"removed"``,
        ``"expired"``, or ``"replaced"``.
        """
        if self._discovery:
            self._discovery.set_callbacks(on_up=on_up, on_down=on_down)
        return self

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> "PeerLink":
        return self.start()

    def __exit__(self, *_: Any) -> None:
        self.stop()

    # ── Unicast call ──────────────────────────────────────────────────────────

    def _call_one(
        self,
        target: str,
        func_name: str,
        *args: Any,
        timeout: float,
        transport: str,
        **kwargs: Any,
    ) -> Any:
        if not self._discovery:
            raise PeerLinkError("Node is not started")
        info = self._discovery.resolve(target)
        if info is None:
            raise PeerNotFound(
                f"Peer '{target}' not found.  Known: {self._discovery.peers()}"
            )

        call_id = str(uuid.uuid4())
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

        # Transport selection
        chosen = transport
        if chosen == "auto":
            chosen = "udp" if len(payload) <= MAX_SAFE_UDP_PAYLOAD else "tcp"

        if chosen == "tcp":
            return self._call_over_tcp(info, payload, timeout)
        else:
            return self._call_over_udp(info, payload, call_id, timeout)

    # ── UDP unicast ───────────────────────────────────────────────────────────

    def _call_over_udp(
        self, info: PeerInfo, payload: bytes, call_id: str, timeout: float
    ) -> Any:
        event = threading.Event()
        pending = _Pending(call_id=call_id, event=event)
        with self._lock:
            self._pending[call_id] = pending

        try:
            self._udp.send((info.addr, info.port), payload)
        except Exception as exc:
            with self._lock:
                self._pending.pop(call_id, None)
            raise

        fired = event.wait(timeout=timeout)
        with self._lock:
            p = self._pending.pop(call_id, None) or pending
        reply = p.result

        if not fired or reply is None:
            raise PeerTimeoutError(
                f"RPC '{info.name}.{pending.call_id[:8]}' timed out after {timeout}s"
            )
        return self._extract_result(reply)

    # ── TCP unicast ───────────────────────────────────────────────────────────

    def _call_over_tcp(
        self, info: PeerInfo, payload: bytes, timeout: float
    ) -> Any:
        """
        Connect, send framed request, read framed reply, disconnect.

        Each call opens a fresh connection.  For repeated large-payload calls
        to the same peer consider keeping the connection alive (future work).
        """
        fs = self._tcp.connect((info.addr, info.tcp_port), timeout=timeout)
        try:
            fs.send_frame(payload)
            raw = fs.recv_frame()
            if not raw:
                raise PeerLinkError("TCP peer closed connection before replying")
            reply = json.loads(raw.decode("utf-8"))
            return self._extract_result(reply)
        finally:
            fs.close()

    # ── Broadcast ─────────────────────────────────────────────────────────────

    def _call_all(
        self,
        func_name: str,
        *args: Any,
        timeout: float,
        transport: str,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Fan-out RPC to all known peers in parallel; returns {name: result_or_exc}."""
        names = self._discovery.peers() if self._discovery else []
        results: Dict[str, Any] = {}
        lock = threading.Lock()

        def worker(name: str) -> None:
            try:
                res = self._call_one(
                    name, func_name, *args,
                    timeout=timeout, transport=transport, **kwargs
                )
                with lock:
                    results[name] = res
            except Exception as exc:
                with lock:
                    results[name] = exc

        threads = [
            threading.Thread(target=worker, args=(n,), daemon=True)
            for n in names
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout + 1)
        return results

    # ── Inbound UDP message handling ──────────────────────────────────────────

    def _on_udp_message(self, msg: dict, addr: Addr) -> None:
        msg_type = msg.get("type")
        if msg_type == "request":
            # Dispatch in executor so the recv thread never blocks
            ctx = contextvars.copy_context()
            self._executor.submit(ctx.run, self._dispatch_rpc, msg, addr, "udp")
        elif msg_type in ("response", "error"):
            self._complete_pending(msg)

    def _complete_pending(self, msg: dict) -> None:
        call_id = msg.get("id")
        if not call_id:
            return
        with self._lock:
            pending = self._pending.get(call_id)
        if pending:
            pending.result = msg
            pending.event.set()

    # ── Inbound TCP connection handling ───────────────────────────────────────

    def _on_tcp_connection(self, fs: _FrameSocket, addr: Addr) -> None:
        """
        Handles one inbound TCP connection from a peer.

        Reads a single request frame, dispatches the RPC synchronously (the
        handler runs in the accept thread — simple for MVP), writes reply frame.
        Extending to persistent connections is straightforward.
        """
        raw = fs.recv_frame()
        if not raw:
            return
        try:
            msg = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return

        if msg.get("type") != "request":
            return

        reply = self._invoke_handler(msg)
        try:
            fs.send_frame(json.dumps(reply).encode("utf-8"))
        except OSError:
            pass

    # ── Handler invocation (shared between UDP + TCP dispatch) ───────────────

    def _dispatch_rpc(self, msg: dict, addr: Addr, proto: str) -> None:
        """Run in executor.  Sends reply via the same protocol."""
        reply = self._invoke_handler(msg)
        payload = json.dumps(reply).encode("utf-8")

        if proto == "udp":
            try:
                # UDP reply: use source address, not registered port
                self._udp.send(addr, payload)
            except Exception as exc:
                logger.warning("UDP reply failed: %s", exc)

    def _invoke_handler(self, msg: dict) -> dict:
        call_id = msg.get("id")
        func_name = msg.get("rpc", "")
        args = msg.get("args", [])
        kwargs = msg.get("kwargs", {})
        caller = msg.get("src")
        if isinstance(caller, bytes):
            caller = caller.decode("utf-8", errors="replace")

        handler = self._handlers.get(func_name)
        if handler is None:
            return {
                "type": "error",
                "id": call_id,
                "error": {
                    "type": "RemoteError",
                    "message": f"No function '{func_name}' registered on {self.node_name}",
                },
            }
        try:
            result = run_with_current_peer(caller, handler, *args, **kwargs)
            return {"type": "response", "id": call_id, "result": result}
        except Exception as exc:
            return {
                "type": "error",
                "id": call_id,
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }

    # ── Result extraction ─────────────────────────────────────────────────────

    @staticmethod
    def _extract_result(reply: dict) -> Any:
        if reply.get("type") == "error" or "error" in reply:
            err = reply.get("error")
            if isinstance(err, dict):
                raise RemoteError(f"{err.get('type')}: {err.get('message')}")
            raise RemoteError(str(err))
        return reply.get("result")

    # ── Logging ───────────────────────────────────────────────────────────────

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(f"[{time.strftime('%H:%M:%S')}][{self.node_name}] {msg}")

    @staticmethod
    def _configure_logger() -> None:
        if not logger.handlers:
            h = logging.StreamHandler()
            h.setFormatter(
                logging.Formatter(
                    "[%(asctime)s][peerlink][%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S",
                )
            )
            logger.addHandler(h)
        logger.setLevel(logging.INFO)


# ─────────────────────────────────────────────────────────────────────────────
# SwarmNode — auto-starts on construction
# ─────────────────────────────────────────────────────────────────────────────

class SwarmNode(PeerLink):
    """
    ``PeerLink`` that calls ``start()`` in ``__init__``.

    Designed for interactive / scripting use where you want a node ready
    without a ``with`` block.  Always pair with an explicit ``stop()`` call
    or use as a context manager.

    Example::

        node = SwarmNode("Worker1")
        node.register("ping", lambda: "pong")
        node.wait_for_peers(2)
        results = node.call("ALL", "ping")
        node.stop()
    """

    def __init__(self, node_name: str, **kwargs: Any) -> None:
        super().__init__(node_name, **kwargs)
        self.start()
