# PeerLink 🚀
**Zero-config P2P RPC for Python LAN Applications**

PeerLink is a lightweight, zero-configuration peer-to-peer RPC library designed for local network (LAN/WiFi) communication. It uses mDNS (Zeroconf) for automatic discovery and provides both UDP and TCP transports for flexible, efficient, and reliable messaging.

[![PyPI version](https://img.shields.io/pypi/v/peerlink.svg)](https://pypi.org/project/peerlink/)
[![Python versions](https://img.shields.io/pypi/pyversions/peerlink.svg)](https://pypi.org/project/peerlink/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## ✨ Key Features (v1.2.0)

- **Zero-Config Discovery**: Automatic peer finding using mDNS. Just name your node and start.
- **Dual Transport (UDP + TCP)**: 
    - **UDP**: Fast, fire-and-forget for small RPCs (≤ 1,200 bytes).
    - **TCP**: Reliable, ordered delivery for large payloads (> 1,200 bytes) or explicit blobs.
- **Modular Architecture**: Re-engineered for stability with focused modules for discovery, transport, and async support.
- **Ergonomic Proxy API**: Call remote methods as if they were local: `node.peer("NodeB").compute(data)`.
- **Async First**: Full support for `asyncio` with `AsyncPeerLink`.
- **Context Awareness**: Access the caller's name inside handlers using `current_peer.get()`.
- **Robustness**: Proper TTL-based pruning, deterministic port derivation, and clear, actionable error messages.

---

## 🚀 Quick Start

### 1. Installation
```bash
pip install peerlink
```

### 2. Basic Usage (Synchronous)
**Node A (Listener)**
```python
from peerlink import PeerLink

def add(a, b):
    return a + b

with PeerLink("NodeA") as node:
    node.register("add", add)
    print("NodeA is running...")
    node.run_forever()
```

**Node B (Caller)**
```python
from peerlink import PeerLink

with PeerLink("NodeB") as node:
    node.wait_for_peers(1)
    result = node.peer("NodeA").add(10, 20)
    print(f"Result: {result}") # Output: 30
```

### 3. Async Usage
```python
import asyncio
from peerlink import AsyncPeerLink

async def main():
    async with AsyncPeerLink("NodeC") as node:
        await node.wait_for_peers(1)
        # Use _transport="tcp" for large data
        result = await node.peer("NodeA").add(5, 5, _transport="tcp")
        print(f"Async Result: {result}")

asyncio.run(main())
```

---

## 🛠 Advanced Features

### Explicit Transport Selection
PeerLink automatically picks UDP for small messages and TCP for large ones. You can override this:
```python
# Force TCP for high reliability
node.call("NodeA", "add", 1, 2, transport="tcp")

# Using the proxy (underscore prefix avoids conflict with remote kwarg names)
node.peer("NodeA").add(1, 2, _transport="tcp", _timeout=10.0)
```

### Caller Identity
Handlers can identify who is calling them:
```python
from peerlink import current_peer

def secure_action():
    caller = current_peer.get()
    if caller == "Admin":
        return "Secret Data"
    return "Permission Denied"
```

### SwarmNode (For Scripts & Notebooks)
A convenience wrapper that starts immediately without a `with` block:
```python
from peerlink import SwarmNode
node = SwarmNode("QuickNode")
# Ready to use immediately!
```

---

## 📝 Architecture Overview

PeerLink v1.2.0 is built on a clean, modular foundation:

- **`discovery.py`**: mDNS publishing and browsing. Maintains a clean peer cache with automatic TTL pruning.
- **`transport.py`**: Handles raw UDP datagrams and length-prefixed TCP frames.
- **`node.py` / `async_node.py`**: The high-level API layering RPC logic over the transports.
- **`constants.py`**: Tunable parameters like `MAX_SAFE_UDP_PAYLOAD` (1,200 bytes) and `PEER_TTL` (60s).

For a deep dive into the internals, see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 🚥 Command Line Interface

PeerLink includes a handy CLI for debugging your network.

```bash
# Discover all peers on the network
peerlink discover

# Ping a peer to test connectivity (supports --transport auto|udp|tcp)
peerlink ping NodeA --transport tcp
```

---

## ⚠️ Migration from v1.1.x
Version 1.2.0 introduces some internal changes. Most common usage remains identical, but note:
1. **`on_up` Callback**: Now receives 4 arguments: `(name, addr, udp_port, tcp_port)`.
2. **Exception Name**: `PayloadTooLarge` is now a specific exception for oversized UDP sends.
3. **Removed**: `Channel` and `NativeAsyncPeerLink` have been moved to future extension modules to keep the core MVP lean.

---

## 📄 License
MIT License. See [LICENSE](LICENSE) for details.
