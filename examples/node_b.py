"""
examples/node_b.py
==================
PeerLink v1.2.0  —  NodeB: 2 MB file-sharing demo (receiver side)

Start node_a.py first, then run this script in a *separate terminal/process*.

What this script does:
  1. Starts NodeB and registers a "receive_file" handler.
  2. Waits until NodeA appears on the LAN (mDNS discovery, up to 30 s).
  3. Receives the 2 MB file from NodeA, verifies MD5, and acknowledges.
  4. As a bonus round, NodeB pushes its own 2 MB payload back to NodeA
     via TCP — demonstrating bidirectional large-payload transfer.

Usage:
    python examples/node_b.py
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
PEER_NAME = "NodeA"
MY_NAME   = "NodeB"
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    print(f"[NodeB] PeerLink v1.2.0 — 2 MB file-transfer demo (receiver)")

    received_event = __import__("threading").Event()
    received_reply: dict = {}

    # ── Handlers registered on NodeB ─────────────────────────────────────────
    def receive_file(filename: str, data_b64: str, sender_md5: str) -> dict:
        """Accept a file from any peer and verify its integrity."""
        raw = base64.b64decode(data_b64)
        our_md5 = hashlib.md5(raw).hexdigest()
        ok = our_md5 == sender_md5
        print(
            f"\n[NodeB] ← Received '{filename}': "
            f"{len(raw):,} bytes  MD5={'✓ match' if ok else '✗ MISMATCH!'}"
        )
        received_reply["ok"] = ok
        received_reply["md5"] = our_md5
        received_reply["size"] = len(raw)
        received_event.set()
        return {"ok": ok, "md5": our_md5, "size": len(raw)}

    # ── Start node ───────────────────────────────────────────────────────────
    with PeerLink(MY_NAME) as node:
        node.register("receive_file", receive_file)

        print(f"[NodeB] Online. Waiting for {PEER_NAME} (up to 30 s) …")
        found = node.wait_for_peers(1, timeout=30.0)
        if not found:
            print(f"[NodeB] ✗ {PEER_NAME} not found. Is node_a.py running?")
            return

        peers = node.peer_names()
        print(f"[NodeB] Discovered peers: {peers}")

        # ── Wait to receive the file from NodeA ──────────────────────────────
        print(f"[NodeB] Ready. Waiting for NodeA to push a 2 MB file …")
        received_event.wait(timeout=60.0)

        if not received_event.is_set():
            print("[NodeB] ✗ Did not receive a file within 60 s.")
            return

        print(f"[NodeB] File received: {received_reply}")

        # ── Push a file BACK to NodeA (bidirectional demo) ───────────────────
        print(f"\n[NodeB] Generating {FILE_SIZE_BYTES // 1024 // 1024} MB payload for push-back …")
        payload_bytes = os.urandom(FILE_SIZE_BYTES)
        payload_md5   = hashlib.md5(payload_bytes).hexdigest()
        payload_b64   = base64.b64encode(payload_bytes).decode("ascii")

        print(f"[NodeB] → Pushing {FILE_SIZE_BYTES:,} bytes to {PEER_NAME} via TCP …")
        t0 = time.perf_counter()
        try:
            reply = node.call(
                PEER_NAME,
                "receive_file",
                "nodeb_2mb.bin",
                payload_b64,
                payload_md5,
                transport="tcp",
                timeout=60.0,
            )
        except Exception as exc:
            print(f"[NodeB] ✗ Push-back failed: {exc}")
            return

        elapsed_ms = (time.perf_counter() - t0) * 1000
        throughput_mbps = (FILE_SIZE_BYTES * 8) / (elapsed_ms / 1000) / 1_000_000

        print(f"[NodeB] ✓ Push-back complete in {elapsed_ms:.1f} ms "
              f"({throughput_mbps:.1f} Mbps)")
        print(f"[NodeB]   NodeA says: {reply}")

        if reply.get("ok"):
            print("[NodeB] ✓ MD5 verified by NodeA — bidirectional transfer confirmed.")
        else:
            print("[NodeB] ✗ MD5 mismatch reported by NodeA!")

        time.sleep(2)

    print("[NodeB] Stopped.")


if __name__ == "__main__":
    main()
