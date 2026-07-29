"""
peerlink.transport
~~~~~~~~~~~~~~~~~~
Protocol-agnostic transport layer.

Two transports are provided:

``UDPTransport``
    Fire-and-forget datagrams.  Fast for small RPC payloads (< 1200 B).
    Each call is a single ``sendto``; replies are matched by ``call_id``.

``TCPTransport``
    Reliable, ordered byte stream.  Suitable for large payloads, file
    transfer, and streaming.  Each frame is prefixed with a 4-byte
    big-endian length header.

Both implement the same interface:

.. code-block:: python

    transport.send(addr, data)          # fire-and-forget (UDP) / framed write (TCP)
    transport.recv(timeout) -> bytes    # blocking read of one message/frame
    transport.start() / stop()

``RPCTransport`` (composition)
    The main node uses this to dispatch outbound RPCs over UDP and receive
    replies, while large payloads or streams route through TCP.
"""

from __future__ import annotations

import json
import queue
import socket
import struct
import threading
from typing import Callable, Dict, Optional, Tuple

from .constants import (
    MAX_DATAGRAM,
    MAX_SAFE_UDP_PAYLOAD,
    RPC_EXECUTOR_MAX_WORKERS,
    TCP_FRAME_HEADER_SIZE,
    TCP_MAX_FRAME,
)
from .exceptions import PayloadTooLarge, PeerLinkError, PeerTimeoutError
from ._utils import reject_if_too_large

__all__ = ["UDPTransport", "TCPTransport"]

Addr = Tuple[str, int]
MsgHandler = Callable[[dict, Addr], None]


# ─────────────────────────────────────────────────────────────────────────────
# UDP
# ─────────────────────────────────────────────────────────────────────────────

class UDPTransport:
    """
    Thin wrapper around a single non-blocking UDP socket.

    *on_message* is called from the receive thread for every well-formed
    JSON datagram.  Malformed datagrams are silently dropped — UDP has no
    error signalling.

    Thread model: one background thread calls ``recvfrom`` in a loop and
    dispatches to *on_message*.  ``send`` is thread-safe; it acquires
    ``_send_lock`` before calling ``sendto``.
    """

    def __init__(self, port: int, on_message: MsgHandler) -> None:
        self._port = port
        self._on_message = on_message
        self._sock: Optional[socket.socket] = None
        self._send_lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
        self._sock.bind(("0.0.0.0", self._port))
        self._sock.settimeout(0.5)
        self._running = True
        self._thread = threading.Thread(
            target=self._recv_loop, daemon=True, name=f"peerlink-udp-{self._port}"
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    # ── I/O ───────────────────────────────────────────────────────────────────

    def send(self, addr: Addr, data: bytes) -> None:
        """
        Send *data* to *addr*.

        Raises :exc:`PayloadTooLarge` if data exceeds ``MAX_SAFE_UDP_PAYLOAD``.
        Raises :exc:`PeerLinkError` if the transport is stopped.
        """
        if not self._sock:
            raise PeerLinkError("UDP transport is not started")
        reject_if_too_large(data, "UDP send")
        with self._send_lock:
            self._sock.sendto(data, addr)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _recv_loop(self) -> None:
        while self._running:
            try:
                data, addr = self._sock.recvfrom(MAX_DATAGRAM)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                msg = json.loads(data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            try:
                self._on_message(msg, addr)
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# TCP
# ─────────────────────────────────────────────────────────────────────────────

class _FrameSocket:
    """
    Length-prefixed frame protocol on top of a raw TCP socket.

    Frame wire format:
        [4 bytes big-endian payload length][payload bytes]
    """

    HEADER = struct.Struct(">I")    # unsigned 4-byte big-endian

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock

    def send_frame(self, payload: bytes) -> None:
        header = self.HEADER.pack(len(payload))
        self._sock.sendall(header + payload)

    def recv_frame(self) -> bytes:
        """Block until one complete frame is received.  Returns b'' on EOF."""
        header = self._recv_exact(TCP_FRAME_HEADER_SIZE)
        if not header:
            return b""
        (length,) = self.HEADER.unpack(header)
        if length > TCP_MAX_FRAME:
            raise PeerLinkError(
                f"TCP frame too large: {length} > TCP_MAX_FRAME ({TCP_MAX_FRAME})"
            )
        return self._recv_exact(length)

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                return b""
            buf += chunk
        return bytes(buf)

    def close(self) -> None:
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass


class TCPTransport:
    """
    Server-side TCP listener.  Accepts connections and calls
    *on_connection(framed_socket, peer_addr)* in a daemon thread per client.

    ``connect(addr)`` opens an outbound connection and returns a
    :class:`_FrameSocket` for the caller to use directly.
    """

    def __init__(
        self,
        port: int,
        on_connection: Callable[[_FrameSocket, Addr], None],
    ) -> None:
        self._port = port
        self._on_connection = on_connection
        self._sock: Optional[socket.socket] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("0.0.0.0", self._port))
        self._sock.listen(16)
        self._sock.settimeout(0.5)
        self._running = True
        self._thread = threading.Thread(
            target=self._accept_loop, daemon=True, name=f"peerlink-tcp-{self._port}"
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    # ── Outbound ──────────────────────────────────────────────────────────────

    def connect(self, addr: Addr, timeout: float = 5.0) -> _FrameSocket:
        """
        Open a TCP connection to *addr* and return a :class:`_FrameSocket`.

        The caller owns the socket and must call ``close()`` when done.
        """
        raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw.settimeout(timeout)
        try:
            raw.connect(addr)
        except OSError as exc:
            raw.close()
            raise PeerLinkError(f"TCP connect to {addr} failed: {exc}") from exc
        raw.settimeout(None)    # switch to blocking after connect
        return _FrameSocket(raw)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _accept_loop(self) -> None:
        while self._running:
            try:
                client_sock, addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            fs = _FrameSocket(client_sock)
            t = threading.Thread(
                target=self._handle, args=(fs, addr), daemon=True
            )
            t.start()

    def _handle(self, fs: _FrameSocket, addr: Addr) -> None:
        try:
            self._on_connection(fs, addr)
        except Exception:
            pass
        finally:
            fs.close()
