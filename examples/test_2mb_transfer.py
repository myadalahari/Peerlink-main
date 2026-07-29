"""
examples/test_2mb_transfer.py
==============================
PeerLink v1.2.0 — Automated in-process 2 MB file-transfer test.

Runs NodeA and NodeB **in the same Python process** on loopback so you can
verify the TCP large-payload path without two separate terminals.

What it tests:
  1. Both nodes start and discover each other via mDNS.
  2. NodeA sends a 2 MB random payload to NodeB over TCP (transport="tcp").
  3. NodeB receives it, verifies the MD5, and returns an acknowledgement.
  4. NodeB pushes 2 MB back to NodeA (bidirectional).
  5. Pass/fail verdict printed at the end.

Usage:
    cd <repo-root>
    pip install -e ".[dev]"          # or: pip install zeroconf click
    python examples/test_2mb_transfer.py

Expected output (times will vary):
    [TEST] PeerLink v1.2.0 — in-process 2 MB TCP transfer test
    ...
    [TEST] ✓ PASS — 2 MB A→B in 45.2 ms (354.9 Mbps)
    [TEST] ✓ PASS — 2 MB B→A in 43.8 ms (365.5 Mbps)
    [TEST] All assertions passed.
"""

import base64
import hashlib
import os
import sys
import threading
import time

# Make sure the local src/ tree is importable when running from repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from peerlink import PeerLink, __version__

FILE_SIZE = 2 * 1024 * 1024   # 2 MB

# ─────────────────────────────────────────────────────────────────────────────
# Shared state for the test
# ─────────────────────────────────────────────────────────────────────────────
results: dict = {
    "a_to_b_ok":         False,
    "a_to_b_ms":         0.0,
    "b_to_a_ok":         False,
    "b_to_a_ms":         0.0,
    "b_received_event":  threading.Event(),
    "a_received_event":  threading.Event(),
    "node_b_reply":      {},
    "node_a_reply":      {},
}


# ─────────────────────────────────────────────────────────────────────────────
# Handler factories
# ─────────────────────────────────────────────────────────────────────────────

def make_receiver(label: str, event_key: str, reply_key: str):
    """Return a receive_file handler that sets an event and stores the result."""
    def receive_file(filename: str, data_b64: str, sender_md5: str) -> dict:
        raw = base64.b64decode(data_b64)
        our_md5 = hashlib.md5(raw).hexdigest()
        ok = our_md5 == sender_md5
        print(
            f"[{label}] ← '{filename}': {len(raw):,} bytes  "
            f"MD5 {'✓' if ok else '✗ MISMATCH'}"
        )
        results[reply_key] = {"ok": ok, "md5": our_md5, "size": len(raw)}
        results[event_key].set()
        return results[reply_key]
    return receive_file


# ─────────────────────────────────────────────────────────────────────────────
# Main test
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"[TEST] PeerLink v{__version__} — in-process 2 MB TCP transfer test")
    print(f"[TEST] Payload size: {FILE_SIZE:,} bytes ({FILE_SIZE // 1024 // 1024} MB)")

    # ── Build payloads ────────────────────────────────────────────────────────
    payload_a = os.urandom(FILE_SIZE)
    md5_a     = hashlib.md5(payload_a).hexdigest()
    b64_a     = base64.b64encode(payload_a).decode("ascii")

    payload_b = os.urandom(FILE_SIZE)
    md5_b     = hashlib.md5(payload_b).hexdigest()
    b64_b     = base64.b64encode(payload_b).decode("ascii")

    print(f"[TEST] NodeA payload MD5: {md5_a}")
    print(f"[TEST] NodeB payload MD5: {md5_b}")

    # ── Start both nodes ──────────────────────────────────────────────────────
    node_a = PeerLink("NodeA")
    node_b = PeerLink("NodeB")

    node_a.register(
        "receive_file",
        make_receiver("NodeA", "a_received_event", "node_a_reply"),
    )
    node_b.register(
        "receive_file",
        make_receiver("NodeB", "b_received_event", "node_b_reply"),
    )

    node_a.start()
    node_b.start()

    try:
        # ── Discovery ────────────────────────────────────────────────────────
        print("[TEST] Waiting for mutual discovery (up to 20 s) …")
        t_disc = time.perf_counter()
        ok_a = node_a.wait_for_peers(1, timeout=20.0)
        ok_b = node_b.wait_for_peers(1, timeout=20.0)
        disc_ms = (time.perf_counter() - t_disc) * 1000

        if not (ok_a and ok_b):
            print(f"[TEST] ✗ Discovery failed (A:{ok_a} B:{ok_b}). Aborting.")
            return

        print(f"[TEST] Peers discovered in {disc_ms:.0f} ms")
        print(f"[TEST]   NodeA sees: {node_a.peer_names()}")
        print(f"[TEST]   NodeB sees: {node_b.peer_names()}")

        # ── A → B (2 MB via TCP) ─────────────────────────────────────────────
        print(f"\n[TEST] Sending 2 MB  NodeA → NodeB  (transport=tcp) …")
        t0 = time.perf_counter()
        reply = node_a.call(
            "NodeB", "receive_file",
            "nodea_2mb.bin", b64_a, md5_a,
            transport="tcp",
            timeout=120.0,
        )
        elapsed_a2b = (time.perf_counter() - t0) * 1000
        mbps_a2b = (FILE_SIZE * 8) / (elapsed_a2b / 1000) / 1_000_000

        ok_a2b = reply.get("ok", False)
        results["a_to_b_ok"] = ok_a2b
        results["a_to_b_ms"] = elapsed_a2b

        status = "✓ PASS" if ok_a2b else "✗ FAIL"
        print(f"[TEST] {status} — 2 MB A→B in {elapsed_a2b:.1f} ms ({mbps_a2b:.1f} Mbps)")

        # ── B → A (2 MB via TCP) ─────────────────────────────────────────────
        print(f"\n[TEST] Sending 2 MB  NodeB → NodeA  (transport=tcp) …")
        t1 = time.perf_counter()
        reply2 = node_b.call(
            "NodeA", "receive_file",
            "nodeb_2mb.bin", b64_b, md5_b,
            transport="tcp",
            timeout=120.0,
        )
        elapsed_b2a = (time.perf_counter() - t1) * 1000
        mbps_b2a = (FILE_SIZE * 8) / (elapsed_b2a / 1000) / 1_000_000

        ok_b2a = reply2.get("ok", False)
        results["b_to_a_ok"] = ok_b2a
        results["b_to_a_ms"] = elapsed_b2a

        status2 = "✓ PASS" if ok_b2a else "✗ FAIL"
        print(f"[TEST] {status2} — 2 MB B→A in {elapsed_b2a:.1f} ms ({mbps_b2a:.1f} Mbps)")

        # ── Final verdict ─────────────────────────────────────────────────────
        print()
        if ok_a2b and ok_b2a:
            print("[TEST] ✓ All assertions passed.")
            print("[TEST]   • TCP large-payload RPC: working")
            print("[TEST]   • MD5 integrity (A→B):   verified")
            print("[TEST]   • MD5 integrity (B→A):   verified")
            print("[TEST]   • Bidirectional 2 MB:    confirmed")
        else:
            print("[TEST] ✗ One or more assertions FAILED.")
            print(f"[TEST]   A→B ok={ok_a2b}  B→A ok={ok_b2a}")
            sys.exit(1)

    finally:
        node_a.stop()
        node_b.stop()
        print("[TEST] Both nodes stopped.")


if __name__ == "__main__":
    main()
