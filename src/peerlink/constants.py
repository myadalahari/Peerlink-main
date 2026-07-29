"""
peerlink.constants
~~~~~~~~~~~~~~~~~~
Single source of truth for every tunable value.
Importing anything else must not depend on this module having side-effects.
"""

__version__ = "1.2.0"

# ── mDNS ────────────────────────────────────────────────────────────────────
SERVICE_TYPE = "_peerlink._tcp.local."

# ── Ports ───────────────────────────────────────────────────────────────────
BASE_PORT = 49152          # Start of IANA dynamic/ephemeral range
PORT_RANGE = 16383         # 49152–65535

# ── UDP ─────────────────────────────────────────────────────────────────────
# Physical MTU 1500 minus IPv4 (20) + UDP (8) headers + JSON framing overhead.
# Fragmented UDP is frequently dropped on real LAN hardware.  Do NOT raise this
# to silence errors; instead switch to TCP for large payloads.
MAX_SAFE_UDP_PAYLOAD = 1200
MAX_DATAGRAM = 65535       # Hard OS limit; only used for internal guard assertions

# ── TCP ─────────────────────────────────────────────────────────────────────
# Frame header is 4 bytes big-endian length prefix.
TCP_FRAME_HEADER_SIZE = 4
TCP_MAX_FRAME = 4 * 1024 * 1024    # 4 MB; reject frames larger than this on recv

# ── Timeouts / intervals ────────────────────────────────────────────────────
RPC_TIMEOUT = 5.0          # Default per-call timeout (seconds)
PEER_TTL = 60.0            # Peer cache entry lifetime
DISCOVERY_WAIT = 3.0       # Seconds to let mDNS settle after start()
CONNECT_TIMEOUT = 5.0      # TCP connect() timeout
TCP_READ_TIMEOUT = 10.0    # TCP blocking read timeout

# ── Concurrency ─────────────────────────────────────────────────────────────
RPC_EXECUTOR_MAX_WORKERS = 16
