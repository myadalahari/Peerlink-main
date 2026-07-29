# Changelog

## [1.2.0] - 2026-04-07

### Summary
Full MVP rewrite — TCP + UDP dual-transport, modular architecture. Same public API,
four bugs fixed, nine focused modules (total ~1,140 lines vs. 1,878-line monolith).

### Added
- **Dual-transport, one parameter** — `transport='auto' | 'udp' | 'tcp'` on every
  `call()` and `peer().fn()` invocation. `'auto'` selects UDP for payloads ≤ 1,200 B
  and TCP for larger ones automatically.
- **TCP RPC dispatch** — `TCPTransport` now fully dispatches inbound RPC requests
  (fixed: TCP server existed in v1.1.x but silently dropped all RPC frames).
- **`PayloadTooLarge` exception** — distinct subtype of `PeerLinkError` with an
  actionable message and a suggestion to switch to TCP; replaces the generic error.
- **`PeerNotFound` lists known peers** — e.g.
  `PeerNotFound: Peer 'Host' not found. Known: ['NodeA', 'Worker1']`.
- **`_FrameSocket`** — internal helper in `transport.py`: 4-byte big-endian length
  prefix framing with `send_frame` / `recv_frame` and `_recv_exact` for partial-read
  reassembly.
- **`discovery.py`** — standalone module with `on_up(name, addr, udp_port, tcp_port)`
  (4-arg signature; was 3-arg).
- **Modular file layout** — nine files each under 300 lines, single responsibility:
  `constants.py`, `exceptions.py`, `_utils.py`, `discovery.py`, `transport.py`,
  `node.py`, `async_node.py`, `cli.py`, `__init__.py`.
- **CLI `--transport` flag** — `peerlink ping NodeB --transport tcp` tests each
  path; `discover` output uses bullet characters and reports elapsed time in ms.

### Fixed
- **Bug 1 — Double payload size guard** (`core.py` lines 1763–1770): the oversized
  payload was registered in `_pending` before either guard ran, leaking the entry on
  error. Fix: single guard inside `UDPTransport.send()` *before* `_pending` is touched.
- **Bug 2 — TCP RPC path was dead**: `_start_tcp_server` accepted connections but the
  handler only handled `stream` / `file` protocols; `type: "request"` frames were
  silently dropped. Fix: `_on_tcp_connection` in `node.py` reads a frame, dispatches
  `_invoke_handler`, and writes the reply frame.
- **Bug 3 — asyncio wrong-loop error** in `async_udp.py`: `self._loop` was captured
  at `start()` time; recreating the loop caused `Future belongs to a different event
  loop`. Fix: call `asyncio.get_running_loop()` at each `call()` site instead.
- **Bug 4 — `PeerProxy.__getattr__` intercepted dunder names**: `__deepcopy__`,
  `__getstate__`, etc. triggered remote calls. Fix: guard with
  `if func_name.startswith('__') and func_name.endswith('__'): raise AttributeError`.

### Changed
- `on_up` callback signature: `(name, addr, udp_port, tcp_port)` — **breaking**;
  was `(name, addr, port)` in v1.1.x.
- `SwarmNode.__init__` wraps `OSError` from port bind with descriptive context
  instead of surfacing a bare `OSError`.
- Verbose logging no longer duplicates `StreamHandler` when multiple nodes share
  one process (uses `if not logger.handlers` guard correctly).
- `AsyncPeerLink` (non-native path) uses `asyncio.to_thread` for all blocking calls;
  `NativeAsyncPeerLink` is intentionally **not** exported (wrong-loop bug; see above).

### Removed / Not in this release (intentional scope reduction)
- **`Channel` / `DatagramStream`** — unreliable UDP channels mislead users expecting
  stream semantics. Will return as a reliable channel over TCP in a future `streams.py`.
- **`NativeAsyncPeerLink`** — contains the wrong-loop bug; `AsyncPeerLink` (thread
  pool) is safer and adequate for MVP throughput.
- **ARP fallback scan** — 254-thread approach is unsafe on constrained devices; will
  return with a bounded pool or asyncio implementation.
- **`CallResult` / `call_all_results()`** — ergonomic wrapper; re-adds once
  `call('ALL', ...)` is proven stable.
- **File transfer helpers** (`register_file_root`, `get_file`, `put_file`,
  `register_file_handler`) — depend on TCP streaming layer; target `streams.py`.

### Migration from v1.1.x
No import changes needed for the common path — `PeerLink`, `SwarmNode`, `PeerProxy`,
all exceptions, and all constants are exported identically from `peerlink`.

Breaking changes:
- `on_up` callback: add the new `tcp_port` 4th argument.
- `PayloadTooLarge` is a distinct type (catching `PeerLinkError` still works).
- `Channel`, `NativeAsyncPeerLink`, `CallResult`, `call_all_results` are not exported.

---

## [1.1.1] - 2026-03-13

### Added
- **TCP streaming** — deterministic per-node TCP port for long-lived streams alongside existing UDP; `register_stream()` / `open_stream()` provide a `Stream` abstraction with length-prefixed frames over TCP.
- **File transfer over TCP** — minimal HTTP/1.1 subset on the same TCP listener; `register_file_handler()`, `get_file()`, and `put_file()` enable production-grade file transfer using standard semantics (`GET` / `PUT`, `Content-Length`).
- **mDNS TCP advertising** — mDNS TXT now includes `tcp_port` so peers can discover the TCP endpoint for streams and files.

### Changed
- Node startup now reports both UDP and TCP ports; if the TCP bind fails, the node continues in UDP-only mode and logs a warning (no behavior change for existing users).

## [1.1.0] - 2025-03-11

### Added
- **SEQUENCE channel ordering** — `register_channel(..., ordering="SEQUENCE")` and `open_channel(..., ordering="SEQUENCE")` for unreliable sequenced delivery; stale/duplicate datagrams dropped before `recv()`. Ingestion runs inline in the server recv thread (no executor race on `last_seq`).
- **Bounded channel queues** — `open_channel(..., max_queue_size=N, queue_policy="drop_oldest")`; drop-oldest under a single lock with `deque` + `Condition`.
- **Native async UDP** — `NativeAsyncPeerLink` and `AsyncPeerLink(..., native_udp=True)` using `asyncio.create_datagram_endpoint` and Future-based RPC correlation (no per-call `to_thread` on the I/O path).
- **Channel lifecycle** — `channel_close` carries `reason`; `Channel.set_on_disconnect(cb)`; immediate `recv()` wakeup via `_CHANNEL_CLOSED` sentinel and `_ChannelQueue.close()`.
- **Secret-scoped discovery** — `PeerLink(..., secret="...")` derives a realm; mDNS type includes realm; `_node_port(name, realm)` avoids port collisions across secrets.
- **CallResult typing** — `call_all_results()` returns `dict[str, CallResult]` for typed broadcast results.

### Changed
- **MTU enforcement** — `MAX_SAFE_UDP_PAYLOAD = 1200`; oversized sends raise `PeerLinkError`; oversized RPC replies become compact `RemoteError` on the wire.
- **channel_open** wire format includes `instance_id` for session documentation; new channels reset seq windows per `channel_id`.

### Security
- Documented: UDP payloads remain unsigned; `secret=` scopes mDNS only.

## [1.0.0] - earlier
- Initial release with mDNS discovery, UDP RPC, channels, `current_peer`, `AsyncPeerLink` (thread-pool bridge), `CallResult`, and instance_id in peer cache.
