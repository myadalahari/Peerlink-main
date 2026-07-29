"""
Native asyncio UDP transport for PeerLink RPC — no per-call asyncio.to_thread.

Use when the game loop is async and you need 60+ RPC/sec without thread hop
on every call. Discovery still uses zeroconf in a background thread.

Sync wrapper note: if you ever block a sync ``call()`` on async work, use
``asyncio.run_coroutine_threadsafe(native.call(...), loop).result(timeout)``
from the user thread—never ``Future.result()`` from a thread that is not
the loop thread without threadsafe bridge, or you get wrong-loop errors.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import socket
import threading
import time
import uuid
from typing import Any, Callable, Dict, Optional, Tuple

from zeroconf import ServiceBrowser, ServiceInfo, ServiceListener, Zeroconf

from .core import (
    MAX_DATAGRAM,
    MAX_SAFE_UDP_PAYLOAD,
    PEER_TTL,
    RPC_TIMEOUT,
    SERVICE_TYPE,
    PeerInfo,
    PeerLinkError,
    PeerNotFound,
    RemoteError,
    _derive_realm,
    _local_ip,
    _node_port,
    _reject_if_payload_too_large,
    current_peer,
    logger,
    run_with_current_peer,
)

__all__ = ["NativeAsyncPeerLink"]


class _AsyncPeerListener(ServiceListener):
    def __init__(self, node: "NativeAsyncPeerLink"):
        self._node = node

    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zc.get_service_info(type_, name)
        if info:
            self._node._on_service(info)

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        self._node._on_service_removed(name)

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        info = zc.get_service_info(type_, name)
        if info:
            self._node._on_service(info)


class _DatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self, node: "NativeAsyncPeerLink"):
        self._node = node
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        self._node._on_datagram(data, addr)

    def error_received(self, exc: Exception) -> None:
        logger.warning("Async UDP error: %s", exc)


class NativeAsyncPeerLink:
    """
    Async-first PeerLink: RPC via asyncio datagram endpoint (no to_thread
    on the send/recv path). Register handlers with ``register``; ``await
    call(peer, fn, *args)`` resolves when the reply arrives on the same loop.
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
        self._realm = _derive_realm(secret)
        self.port = _node_port(node_name, self._realm)
        self._handlers: Dict[str, Callable[..., Any]] = {
            "__ping__": lambda: {"ok": True, "time": time.time()}
        }
        self._peers: Dict[str, PeerInfo] = {}
        self._pending: Dict[str, asyncio.Future[Any]] = {}
        self._lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional[_DatagramProtocol] = None
        self._zeroconf: Optional[Zeroconf] = None
        self._browser: Optional[ServiceBrowser] = None
        self._started = False

    def _service_type(self) -> str:
        if self._realm:
            return f"_peerlink-{self._realm}._tcp.local."
        return SERVICE_TYPE

    def register(self, name: str, func: Callable[..., Any]) -> "NativeAsyncPeerLink":
        self._handlers[name] = func
        return self

    async def start(self) -> None:
        if self._started:
            return
        self._loop = asyncio.get_running_loop()
        self._protocol = _DatagramProtocol(self)
        self._transport, _ = await self._loop.create_datagram_endpoint(
            lambda: self._protocol,
            local_addr=("0.0.0.0", self.port),
        )
        # mDNS in thread (zeroconf is blocking)
        threading.Thread(target=self._run_zeroconf, daemon=True).start()
        self._started = True

    def _run_zeroconf(self) -> None:
        local_ip = _local_ip()
        st = self._service_type()
        zc = Zeroconf()
        self._zeroconf = zc
        info = ServiceInfo(
            type_=st,
            name=f"{self.node_name}.{st}",
            addresses=[socket.inet_aton(local_ip)],
            port=self.port,
            properties={
                "node": self.node_name,
                "port": str(self.port),
                "instance_id": str(uuid.uuid4()),
            },
            server=f"{self.node_name}.local.",
        )
        zc.register_service(info)
        self._browser = ServiceBrowser(zc, st, _AsyncPeerListener(self))

    def _on_service(self, info: ServiceInfo) -> None:
        props = info.decoded_properties or {}
        peer_name = props.get("node", "")
        if isinstance(peer_name, bytes):
            peer_name = peer_name.decode("utf-8", errors="replace")
        if not peer_name or peer_name == self.node_name:
            return
        addr = socket.inet_ntoa(info.addresses[0]) if info.addresses else None
        if not addr:
            return
        with self._lock:
            self._peers[peer_name] = PeerInfo(
                peer_name, addr, info.port, time.time(), PEER_TTL
            )

    def _on_service_removed(self, mdns_key: str) -> None:
        name = mdns_key.split(".")[0]
        with self._lock:
            self._peers.pop(name, None)

    def _resolve_peer(self, target: str) -> Optional[Tuple[str, int]]:
        t = target.lower()
        with self._lock:
            for name, info in self._peers.items():
                if t in name.lower():
                    return info.addr, info.port
        return None

    def _on_datagram(self, data: bytes, addr: Tuple[str, int]) -> None:
        try:
            msg = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return
        msg_type = msg.get("type")
        if msg_type in {"response", "error"} or "result" in msg or "error" in msg:
            call_id = msg.get("id")
            fut = self._pending.pop(call_id, None) if call_id else None
            if fut and not fut.done():
                if msg.get("type") == "error" or "error" in msg:
                    err = msg.get("error")
                    if isinstance(err, dict):
                        fut.set_exception(
                            RemoteError(f"{err.get('type')}: {err.get('message')}")
                        )
                    else:
                        fut.set_exception(RemoteError(str(err)))
                else:
                    fut.set_result(msg.get("result"))
            return
        if msg_type == "request" or "rpc" in msg:
            if self._loop and self._loop.is_running():
                self._loop.create_task(self._dispatch_request(msg, addr))

    async def _dispatch_request(self, msg: dict, addr: Tuple[str, int]) -> None:
        call_id = msg.get("id")
        func_name = msg.get("rpc", "")
        args = msg.get("args", [])
        kwargs = msg.get("kwargs", {})
        if func_name not in self._handlers:
            resp = {
                "type": "error",
                "id": call_id,
                "error": {
                    "type": "RemoteError",
                    "message": f"No function '{func_name}' registered",
                },
            }
        else:
            try:
                caller = msg.get("src")
                if caller is not None and not isinstance(caller, str):
                    caller = str(caller)

                def _invoke() -> Any:
                    return run_with_current_peer(
                        caller, self._handlers[func_name], *args, **kwargs
                    )

                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    None, contextvars.copy_context().run, _invoke
                )
                resp = {"type": "response", "id": call_id, "result": result}
            except Exception as exc:
                resp = {
                    "type": "error",
                    "id": call_id,
                    "error": {"type": type(exc).__name__, "message": str(exc)},
                }
        payload = json.dumps(resp).encode("utf-8")
        if len(payload) > MAX_SAFE_UDP_PAYLOAD:
            resp = {
                "type": "error",
                "id": call_id,
                "error": {
                    "type": "PeerLinkError",
                    "message": "RPC result too large for UDP",
                },
            }
            payload = json.dumps(resp).encode("utf-8")
        if self._protocol and self._protocol.transport:
            self._protocol.transport.sendto(payload, addr)

    async def call(
        self,
        peer: str,
        func_name: str,
        *args: Any,
        timeout: float = RPC_TIMEOUT,
        **kwargs: Any,
    ) -> Any:
        if not self._started or not self._protocol or not self._protocol.transport:
            raise PeerLinkError("Node not started")
        peer_addr = self._resolve_peer(peer)
        if peer_addr is None:
            raise PeerNotFound(f"Peer '{peer}' not found")
        call_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        self._pending[call_id] = fut
        msg = {
            "type": "request",
            "id": call_id,
            "src": self.node_name,
            "dst": peer,
            "rpc": func_name,
            "args": list(args),
            "kwargs": kwargs,
        }
        payload = json.dumps(msg).encode("utf-8")
        _reject_if_payload_too_large(payload, "async RPC request")
        self._protocol.transport.sendto(payload, peer_addr)
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending.pop(call_id, None)
            from .core import PeerTimeoutError

            raise PeerTimeoutError(
                f"RPC '{func_name}' → '{peer}' timed out after {timeout}s"
            ) from None

    async def stop(self) -> None:
        self._started = False
        if self._browser:
            self._browser.cancel()
        if self._zeroconf:
            try:
                self._zeroconf.close()
            except Exception:
                pass
        if self._transport:
            self._transport.close()
        self._transport = None

    async def wait_for_peers(self, count: int = 1, timeout: float = 30.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if len(self._peers) >= count:
                    return True
            await asyncio.sleep(0.2)
        return False

    async def __aenter__(self) -> "NativeAsyncPeerLink":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()
