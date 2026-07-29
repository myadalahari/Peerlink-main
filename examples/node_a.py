"""
examples/node_a.py
==================
PeerLink v1.2.0  —  NodeA: 2 MB file-sharing demo (sender side)

Run this first, then run node_b.py in a *separate terminal/process*.

What this script does:
  1. Starts NodeA and registers a "receive_file" handler that accepts raw bytes.
  2. Waits until NodeB appears on the LAN (mDNS discovery, up to 30 s).
  3. Generates a 2 MB payload (random bytes to stress real-world data).
  4. Sends the 2 MB payload to NodeB via TCP (transport="tcp") — required because
     2 MB >> MAX_SAFE_UDP_PAYLOAD (1,200 B).
  5. Prints the round-trip time and NodeB's acknowledgement.
  6. Waits for NodeB to optionally echo-back a file of its own (demonstrates
     bidirectional large-payload transfer).

Usage:
    python examples/node_a.py
"""

import os
import sys
import time
import hashlib
import base64

# Make sure the local src/ tree is importable when running from repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from peerlink import PeerLink

# ── Config ────────────────────────────────────────────────────────────────────
FILE_SIZE_BYTES = 2 * 1024 * 1024   # 2 MB
PEER_NAME = "NodeB"
MY_NAME   = "NodeA"
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    print(f"[NodeA] PeerLink v1.2.0 — 2 MB file-transfer demo (sender)")
    print(f"[NodeA] Generating {FILE_SIZE_BYTES // 1024 // 1024} MB payload …")
    payload_bytes = os.urandom(FILE_SIZE_BYTES)
    payload_md5   = hashlib.md5(payload_bytes).hexdigest()
    # Encode as base64 so it travels safely inside JSON
    payload_b64   = base64.b64encode(payload_bytes).decode("ascii")

    print(f"[NodeA] Payload MD5 = {payload_md5}")

    # ── Handlers registered on NodeA ─────────────────────────────────────────
    received_files: dict = {}

    def receive_file(filename: str, data_b64: str, sender_md5: str) -> dict:
        """NodeB can push a file back to NodeA via this handler."""
        raw = base64.b64decode(data_b64)
        our_md5 = hashlib.md5(raw).hexdigest()
        ok = our_md5 == sender_md5
        print(
            f"\n[NodeA] ← Received '{filename}' from {{}}: "
            f"{len(raw):,} bytes  MD5={'✓ match' if ok else '✗ MISMATCH!'}"
        )
        received_files[filename] = raw
        return {"ok": ok, "md5": our_md5, "size": len(raw)}

    # ── Start node ───────────────────────────────────────────────────────────
    with PeerLink(MY_NAME) as node:
        node.register("receive_file", receive_file)

        print(f"[NodeA] Online. Waiting for {PEER_NAME} (up to 30 s) …")
        found = node.wait_for_peers(1, timeout=30.0)
        if not found:
            print(f"[NodeA] ✗ {PEER_NAME} not found within 30 s. Is node_b.py running?")
            return

        peers = node.peer_names()
        print(f"[NodeA] Discovered peers: {peers}")

        # ── Send 2 MB → NodeB ────────────────────────────────────────────────
        print(f"\n[NodeA] → Sending {FILE_SIZE_BYTES:,} bytes to {PEER_NAME} via TCP …")
        t0 = time.perf_counter()
        try:
            reply = node.call(
                PEER_NAME,
                "receive_file",
                "demo_2mb.bin",
                payload_b64,
                payload_md5,
                transport="tcp",   # 2 MB requires TCP; auto would pick this anyway
                timeout=60.0,
            )
        except Exception as exc:
            print(f"[NodeA] ✗ Transfer failed: {exc}")
            return

        elapsed_ms = (time.perf_counter() - t0) * 1000
        throughput_mbps = (FILE_SIZE_BYTES * 8) / (elapsed_ms / 1000) / 1_000_000

        print(f"[NodeA] ✓ Transfer complete in {elapsed_ms:.1f} ms "
              f"({throughput_mbps:.1f} Mbps)")
        print(f"[NodeA]   NodeB says: {reply}")

        if reply.get("ok"):
            print("[NodeA] ✓ MD5 verified by NodeB — data integrity confirmed.")
        else:
            print("[NodeA] ✗ MD5 mismatch reported by NodeB!")

        # ── Stay alive a bit so NodeB can optionally push back ───────────────
        print(f"\n[NodeA] Waiting 15 s for NodeB to push a file back …")
        time.sleep(15)

    print("[NodeA] Stopped.")


if __name__ == "__main__":
    main()
