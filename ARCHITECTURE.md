# PeerLink: How the Module Works (Developer Reference)

This document describes how the PeerLink library works internally so developers have
full insight into architecture, data flow, wire formats, threading, and edge behavior.
It matches the code in **v1.2.0** (MVP modular rewrite).

---

## 1. High-level architecture

PeerLink provides zero-config P2P over LAN using:

1. **mDNS (zeroconf)** — Service discovery: each node publishes a service and browses
   for others. No central server.
2. **UDP** — RPC request/response for small payloads (≤ 1,200 B). Fire-and-forget;
   ~1 RTT, no connection overhead.
3. **TCP** — Reliable request/response for large payloads (> 1,200 B). Guaranteed
   delivery, ordered, ~2 RTT (connect + request). Also used explicitly via
   `transport="tcp"`.

All communication is **by peer name** (e.g. `"Phone1"`). The library resolves names
to `(ip, udp_port, tcp_port)` via the peer cache populated by mDNS. Ports are
**deterministic** from node name (and optional secret/realm) so the same name always
gets the same UDP and TCP ports.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  PeerLink node (e.g. "Phone1")  —  v1.2.0                               │
├─────────────────────────────────────────────────────────────────────────┤
│  mDNS: publish ServiceInfo (udp_port, tcp_port, instance_id, node name) │
│        browse same service type → Discovery._PeerListener → peer cache   │
├─────────────────────────────────────────────────────────────────────────┤
│  UDP socket (UDPTransport):                                              │
│    • RPC: request {type, id, src, dst, rpc, args, kwargs}               │
│           → response/error {type, id, result/error}                     │
│    • Auto-selected when payload ≤ MAX_SAFE_UDP_PAYLOAD (1,200 B)        │
├─────────────────────────────────────────────────────────────────────────┤
│  TCP socket (TCPTransport):                                              │
│    • RPC: same JSON framed with 4-byte big-endian length prefix         │
│    • Auto-selected when payload > 1,200 B, or explicit transport="tcp"  │
│    • _FrameSocket wraps every connection: send_frame / recv_frame       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Module structure (v1.2.0)

The monolithic `core.py` (~1,878 lines) has been split into nine focused files,
each with a single responsibility and under ~300 lines.

```
peerlink/
├── __init__.py       # public re-exports only (~50 lines)
├── constants.py      # every tunable number in one place (~40 lines)
├── exceptions.py     # all exception types (~30 lines)
├── _utils.py         # pure helpers: ports, IP, payload guard, ContextVar (~80 lines)
├── discovery.py      # mDNS publish + browse + peer cache (~210 lines)
├── transport.py      # UDPTransport, TCPTransport, _FrameSocket (~230 lines)
├── node.py           # PeerLink, PeerProxy, SwarmNode (~300 lines)
├── async_node.py     # AsyncPeerLink, AsyncPeerProxy (~130 lines)
└── cli.py            # Click: discover, ping (~70 lines)
```

**Dependency graph** (dependencies flow downward only — no circular imports):

```
constants.py   ← (no imports from peerlink)
exceptions.py  ← (no imports from peerlink)
_utils.py      ← constants, exceptions
discovery.py   ← constants, _utils
transport.py   ← constants, exceptions, _utils
node.py        ← constants, exceptions, _utils, discovery, transport
async_node.py  ← constants, exceptions, node
cli.py         ← constants, exceptions, node
__init__.py    ← all of the above (re-exports only)
```

---

## 3. Constants and configuration

Defined in `constants.py`:

| Constant | Value | Purpose |
|----------|--------|---------|
| `SERVICE_TYPE` | `_peerlink._tcp.local.` | mDNS type (realm variant: `_peerlink-{realm}._tcp.local.`) |
| `BASE_PORT` | 49152 | Start of IANA dynamic range |
| `PORT_RANGE` | 16383 | Ports 49152–65535 |
| `RPC_TIMEOUT` | 5.0 | Default timeout for RPC calls |
| `MAX_DATAGRAM` | 65535 | Hard OS limit; used for internal guard only |
| `MAX_SAFE_UDP_PAYLOAD` | 1200 | Safe single-datagram size; larger payloads auto-switch to TCP |
| `TCP_FRAME_HEADER_SIZE` | 4 | Bytes in the big-endian length prefix |
| `TCP_MAX_FRAME` | 4 MB | Maximum accepted TCP frame to prevent memory attacks |
| `PEER_TTL` | 60.0 | Seconds after which a peer is pruned if not refreshed |
| `DISCOVERY_WAIT` | 3.0 | Default wait (e.g. CLI) before assuming mDNS has settled |
| `CONNECT_TIMEOUT` | 5.0 | TCP connect() timeout |
| `TCP_READ_TIMEOUT` | 10.0 | TCP blocking read timeout |
| `RPC_EXECUTOR_MAX_WORKERS` | 16 | Max threads in the RPC handler pool |

**Port derivation** (in `_utils.py`):

- **UDP**: `derive_port(name, realm)` = `BASE_PORT + (md5(key) % PORT_RANGE)` where
  `key = name` or `realm|name`.
- **TCP**: `derive_port(name, realm, salt="tcp")` — same formula, different key, so
  UDP and TCP ports are stable and distinct per node.
- **Realm**: If `secret` is set, `derive_realm(secret)` = first 16 hex chars of
  `sha256("peerlink|" + secret)`. mDNS service type becomes
  `_peerlink-{realm}._tcp.local.` so only nodes with the same secret see each other.

---

## 4. Node bootstrap (`PeerLink.start()`)

Order of operations:

1. **ThreadPoolExecutor** — `RPC_EXECUTOR_MAX_WORKERS` threads for running RPC
   handlers without blocking the recv loop.
2. **UDP transport** — `UDPTransport(port, on_message)`: `SOCK_DGRAM`, `SO_REUSEADDR`,
   bind to `("", udp_port)`. Recv thread is a daemon.
3. **TCP transport** — `TCPTransport(port, on_connection)`: `SOCK_STREAM`, bind, listen.
   Accept loop in a daemon thread; one daemon thread per accepted connection.
4. **Discovery** — `Discovery(node_name, udp_port, tcp_port)`: creates `Zeroconf`,
   registers `ServiceInfo` (including both ports in TXT properties), starts
   `ServiceBrowser`.

`local_ip()` uses a trick: open a UDP socket, `connect("8.8.8.8", 80)`, then
`getsockname()[0]` to obtain the outward-facing LAN IP without sending any data.

---

## 5. Discovery and peer cache

**`discovery.py` — `Discovery` class**

Published `ServiceInfo`:
- Type: `_peerlink._tcp.local.` or `_peerlink-{realm}._tcp.local.`
- Name: `{node_name}.{type}`
- Addresses: one (local IP from `local_ip()`)
- Port: UDP port (mDNS convention — first port)
- TXT properties: `node`, `port` (UDP), `tcp_port` (TCP), `version`, `instance_id` (UUID per process)

`_PeerListener` callbacks:
- `add_service` / `update_service`: calls `get_service_info`, then `_on_peer_added`.
  `update_service` also refreshes `last_seen` so live peers are not pruned.
- `remove_service`: calls `_on_peer_removed` → fires `on_down` callback.

`_on_peer_added`:
- Skip own name.
- If same name but different `instance_id` → remove stale entry, call `on_down`.
- Insert / update `_peers[peer_name]` = `PeerInfo(name, addr, port, tcp_port, last_seen, instance_id)`.
- Call `on_up(name, addr, udp_port, tcp_port)` — **4 arguments** (breaking change from v1.1.x which passed 3).

**Peer cache pruning**: `_prune_peers()` runs on each mDNS `update_service` and before
`resolve()`. Entries where `now - last_seen > PEER_TTL` are evicted and `on_down` fired.

`resolve(name)` → `PeerInfo | None`: case-insensitive substring match over `_peers`.

**Lifecycle callbacks** (set via `node.set_peer_lifecycle(on_up, on_down)`):
```
on_up(name: str, addr: str, udp_port: int, tcp_port: int) -> None
on_down(name: str, reason: str) -> None   # reason: "removed" | "expired" | "replaced"
```

---

## 6. Transport layer (`transport.py`)

### `UDPTransport`
- Binds `SOCK_DGRAM`, `SO_REUSEADDR`, 1 s socket timeout.
- Recv daemon thread: `recvfrom(MAX_DATAGRAM)` → JSON decode → `on_message(dict, addr)`.
- `send(addr, data)`: raises `PayloadTooLarge` **before** sending if
  `len(data) > MAX_SAFE_UDP_PAYLOAD`.  Single guard, no leaked pending entries.

### `TCPTransport`
- Binds `SOCK_STREAM`, listen.  Accept loop in a daemon thread.
- Each accepted connection → one daemon thread → `on_connection(_FrameSocket, addr)`.
- `connect(addr, timeout)` → `_FrameSocket` for outbound connections.

### `_FrameSocket`
Wraps a raw socket with length-prefixed framing:

```python
HEADER = struct.Struct('>I')   # 4-byte big-endian unsigned int

def send_frame(self, payload: bytes) -> None:
    header = self.HEADER.pack(len(payload))
    self._sock.sendall(header + payload)   # atomic write

def recv_frame(self) -> bytes:
    header = self._recv_exact(4)
    (length,) = self.HEADER.unpack(header)
    return self._recv_exact(length)
```

`_recv_exact(n)` loops on `recv()` until exactly `n` bytes are available, reassembling
partial reads from the kernel buffer.

---

## 7. RPC: request/response flow

### Transport selection (`node._call_one`)

```python
chosen = transport   # 'auto' | 'udp' | 'tcp'
if chosen == 'auto':
    chosen = 'udp' if len(payload) <= MAX_SAFE_UDP_PAYLOAD else 'tcp'

if chosen == 'tcp':
    return self._call_over_tcp(info, payload, timeout)
return self._call_over_udp(info, payload, call_id, timeout)
```

### UDP call path

1. Resolve peer → `PeerInfo`.
2. Generate `call_id = uuid4()`, register `_Pending(call_id, event)` in `_pending`.
3. Build message `{type, id, src, dst, rpc, args, kwargs}`, JSON-encode.
4. `UDPTransport.send(addr, payload)` — raises `PayloadTooLarge` if too big (guard
   fires here, **before** anything is registered — Bug 1 fixed).
5. `event.wait(timeout)`.
6. Pop `_pending[call_id]`, raise `PeerTimeoutError` if not fired.
7. Call `_extract_result(reply)` → return value or raise `RemoteError`.

### TCP call path

1. Resolve peer → `PeerInfo` (uses `info.tcp_port`).
2. `TCPTransport.connect((addr, tcp_port))` → `_FrameSocket`.
3. `fs.send_frame(payload)` → `fs.recv_frame()` → `_extract_result`.
4. `fs.close()` in `finally`.

Each TCP call opens a fresh connection — suitable for large infrequent payloads.

### Inbound UDP handling (`_on_udp_message`)

```python
def _on_udp_message(self, msg, addr):
    if msg['type'] == 'request':
        ctx = contextvars.copy_context()
        self._executor.submit(ctx.run, self._dispatch_rpc, msg, addr, 'udp')
    elif msg['type'] in ('response', 'error'):
        self._complete_pending(msg)
```

`contextvars.copy_context()` propagates `current_peer` into the executor thread so
RPC handlers can call `current_peer.get()` to read the caller's name.

### Inbound TCP handling (`_on_tcp_connection`)

```python
def _on_tcp_connection(self, fs, addr):
    raw = fs.recv_frame()
    msg = json.loads(raw)
    if msg.get('type') != 'request':
        return
    reply = self._invoke_handler(msg)      # runs synchronously in accept thread
    fs.send_frame(json.dumps(reply).encode())
```

**Bug 2 fixed**: the original TCP server silently dropped `type: "request"` frames.
Now `_on_tcp_connection` reads the frame and dispatches it via `_invoke_handler`,
sending the reply over the same connection.

### `_invoke_handler` (shared by UDP + TCP)

1. Look up `_handlers[func_name]`. If missing, return error frame.
2. Run `run_with_current_peer(caller, handler, *args, **kwargs)`.
3. Return `{type: "response", id, result}` on success or
   `{type: "error", id, error: {type, message}}` on exception.

### Broadcast (`call("ALL", ...)`)

- Snapshot peer names. Spawn one daemon thread per peer calling `_call_one`.
- Join threads with `timeout + 1` seconds.
- Return `{peer_name: result_or_exception}`.

---

## 8. `PeerProxy` — attribute-based remote calls

```python
proxy = node.peer("NodeB")
result = proxy.add(1, 2)                        # UDP (auto)
result = proxy.add(1, 2, _transport="tcp")      # TCP
result = proxy.upload(big_data, _transport="tcp", _timeout=10.0)
```

Underscore-prefixed kwargs (`_transport`, `_timeout`) are consumed by `PeerProxy`
and not forwarded to the remote handler.

**Bug 4 fixed**: `__getattr__` now guards dunder names:

```python
def __getattr__(self, func_name: str):
    if func_name.startswith('__') and func_name.endswith('__'):
        raise AttributeError(func_name)   # let stdlib handle it
    def _caller(*args, _timeout=RPC_TIMEOUT, _transport='auto', **kwargs):
        return self._node.call(self._name, func_name, *args,
                               timeout=_timeout, transport=_transport, **kwargs)
    return _caller
```

This prevents `pickle`, `copy`, `pprint` from triggering remote calls on dunder
inspection.

---

## 9. `AsyncPeerLink` — asyncio wrapper

`AsyncPeerLink` holds a sync `PeerLink` internally. All blocking calls are run via
`asyncio.to_thread`:

```python
async with AsyncPeerLink("Player1") as node:
    node.register("tick", game_tick)
    await node.wait_for_peers(1)
    state = await node.peer("Host").tick(inputs)
    blob  = await node.peer("Host").get_map(_transport="tcp")
```

**Bug 3 fixed**: `asyncio.get_running_loop()` is called at each `call()` site, not
captured at `start()` time. This avoids the stale-loop `RuntimeError` on loop
recreation in tests and complex applications.

`NativeAsyncPeerLink` (asyncio datagram endpoint) is **not exported** in v1.2.0 —
it contained the wrong-loop bug and its complexity outweighs the throughput gain for
an MVP. `AsyncPeerLink` (to_thread) is safe and fast enough.

---

## 10. `ContextVar current_peer`

- `current_peer: ContextVar[str]` — set to the caller's node name for the duration
  of each RPC handler invocation.
- `run_with_current_peer(caller, fn, *args, **kwargs)` — sets the var, calls fn,
  restores.
- `contextvars.copy_context().run(...)` propagates the var across executor threads.
- If the handler spawns its own threads, pass the caller name explicitly using
  `run_with_current_peer(current_peer.get(), fn, ...)` in the new thread.

---

## 11. Shutdown (`PeerLink.stop()`)

1. Set `_running = False`.
2. Shutdown RPC executor (`wait=False`).
3. `Discovery.stop()` — cancel browser, unregister `ServiceInfo`, close Zeroconf.
4. `UDPTransport.stop()` — close socket; recv thread exits on next `recvfrom()` timeout.
5. `TCPTransport.stop()` — close listener; accept thread exits on next `accept()` timeout.

---

## 12. `SwarmNode`

Subclass of `PeerLink` that calls `self.start()` in `__init__`. OSError from port
bind is now wrapped with descriptive context (fail loud, fail early). Designed for
interactive / scripting use without explicit `with` blocks.

---

## 13. CLI

| Command | Description |
|---------|-------------|
| `peerlink discover [--wait N]` | Short-lived node, sleep N seconds, bullet-list all discovered peers with elapsed time |
| `peerlink ping NAME [--transport auto\|udp\|tcp] [--timeout N]` | Ping a peer via `__ping__` RPC; exit 1 on failure |

Example output:
```
$ peerlink ping NodeB --transport tcp
Ping → 'NodeB' OK  (12.3 ms via tcp)

$ peerlink discover
Discovered peers:
  • NodeA
  • Worker1
```

---

## 14. Built-in handlers and exceptions

- Every node registers `__ping__` → `lambda: {"ok": True, "ts": time.time()}`.
  Used by `PeerProxy.is_alive()`.
- **`PeerLinkError`** — base exception.
- **`PayloadTooLarge(PeerLinkError)`** — fired by `UDPTransport.send()` with the
  message: `"UDP send: 2048 bytes > MAX_SAFE_UDP_PAYLOAD (1200). Use TCP transport."`.
- **`PeerNotFound(PeerLinkError)`** — includes list of known peers in message.
- **`PeerTimeoutError(TimeoutError, PeerLinkError)`** — RPC did not reply in time.
- **`RemoteError(PeerLinkError)`** — exception raised inside the remote handler.

---

## 15. Summary: where work runs

| Work | Thread / context |
|------|-----------------|
| UDP recv loop | Single daemon thread (`UDPTransport` recv thread) |
| RPC request handling (UDP) | `ThreadPoolExecutor` (`peerlink-rpc-N`) |
| TCP accept loop | Daemon thread (`TCPTransport` accept thread) |
| TCP RPC dispatch | Daemon thread per accepted connection (synchronous) |
| mDNS publish/browse | Zeroconf threads (blocking API) |
| RPC caller (sync `PeerLink`) | User thread (blocked on `event.wait`) |
| `AsyncPeerLink` calls | `asyncio.to_thread` (one thread per call) |

---

## 16. What was intentionally excluded from v1.2.0

These features from `core.py` were deliberately omitted to keep the MVP small and
correct. Each is well-scoped for a follow-on module without touching the core six files.

| Feature | Why excluded |
|---------|-------------|
| `Channel` / `DatagramStream` | Unreliable UDP channels mislead users expecting stream semantics. Revisit as reliable channel over TCP in `streams.py`. |
| `Stream` class (TCP raw byte streams) | Superseded by `_FrameSocket`. Re-expose as a `Stream` facade in a future `streams.py`. |
| ARP fallback scan (`arp_scan`) | 254-thread approach unsafe on constrained devices. Rewrite with bounded pool or asyncio. |
| `NativeAsyncPeerLink` | Contains the wrong-loop bug (Bug 3). `to_thread` approach is safer and adequate. |
| `CallResult` / `call_all_results()` | Ergonomic wrapper — re-add once `call("ALL", ...)` is stable. |
| File transfer (`get_file`, `put_file`, `register_file_root`) | Depends on TCP streaming layer; will land in `streams.py`. |

---

## 17. Migration from v1.1.x

**No-change imports** — the common path is backward compatible:

```python
from peerlink import PeerLink, SwarmNode, PeerProxy
from peerlink import PeerLinkError, PeerNotFound, PeerTimeoutError, RemoteError
from peerlink import DISCOVERY_WAIT, MAX_SAFE_UDP_PAYLOAD, RPC_TIMEOUT
from peerlink import current_peer, run_with_current_peer
```

**Breaking changes:**

- `on_up` callback signature adds `tcp_port` as a 4th argument:
  ```python
  # v1.1.x:  on_up(name, addr, port)
  # v1.2.0:  on_up(name, addr, udp_port, tcp_port)
  ```
- `PayloadTooLarge` is now a distinct exception (still subclasses `PeerLinkError`).
- `Channel`, `NativeAsyncPeerLink`, `CallResult`, `call_all_results` are not exported.

**Example — before and after:**

```python
# v1.1.x basic usage (unchanged in v1.2.0):
with PeerLink("Worker") as node:
    node.register("compute", my_fn)
    node.wait_for_peers(1)
    result = node.peer("Host").compute(data)

# v1.2.0 — large payload via TCP:
with PeerLink("Worker") as node:
    node.register("upload", handle_upload)
    node.wait_for_peers(1)
    result = node.call("Host", "upload", big_bytes, transport="tcp")

# v1.2.0 — async with transport choice:
async with AsyncPeerLink("Player") as node:
    await node.wait_for_peers(1)
    state = await node.peer("Host").tick(inputs)
    blob  = await node.peer("Host").get_map(_transport="tcp")
```

This document reflects the implementation in **PeerLink v1.2.0**.
