"""
peerlink.cli
~~~~~~~~~~~~
Command-line interface.

Commands
--------
``peerlink discover``       List all reachable nodes on the LAN.
``peerlink ping <name>``    Ping a specific node and report round-trip time.
"""

from __future__ import annotations

import time
import uuid

import click

from .constants import DISCOVERY_WAIT
from .exceptions import PeerNotFound
from .node import PeerLink

__all__ = ["cli"]


@click.group()
def cli() -> None:
    """PeerLink command-line interface."""


@cli.command()
@click.option(
    "--wait",
    "wait_seconds",
    default=DISCOVERY_WAIT,
    show_default=True,
    help="Seconds to wait for discovery before printing the peer list.",
)
def discover(wait_seconds: float) -> None:
    """Discover PeerLink nodes on the local network."""
    node_name = f"peerlink-cli-{uuid.uuid4().hex[:6]}"
    with PeerLink(node_name, verbose=False) as node:
        time.sleep(wait_seconds)
        names = node.peer_names()

    if not names:
        click.echo("No peers discovered.")
        return
    click.echo("Discovered peers:")
    for name in names:
        click.echo(f"  • {name}")


@cli.command()
@click.argument("name")
@click.option(
    "--timeout",
    default=5.0,
    show_default=True,
    help="Seconds to wait for a ping reply.",
)
@click.option(
    "--transport",
    default="auto",
    type=click.Choice(["auto", "udp", "tcp"], case_sensitive=False),
    show_default=True,
    help="Transport to use for the ping.",
)
def ping(name: str, timeout: float, transport: str) -> None:
    """Ping a PeerLink node by name and report round-trip time."""
    node_name = f"peerlink-cli-{uuid.uuid4().hex[:6]}"
    with PeerLink(node_name, verbose=False) as node:
        time.sleep(DISCOVERY_WAIT)

        try:
            proxy = node.peer(name)
        except PeerNotFound as exc:
            click.echo(f"Peer '{name}' not found: {exc}", err=True)
            raise SystemExit(1)

        t0 = time.perf_counter()
        alive = proxy.is_alive(timeout=timeout)
        elapsed_ms = (time.perf_counter() - t0) * 1000

    if alive:
        click.echo(f"Ping → '{name}' OK  ({elapsed_ms:.1f} ms via {transport})")
    else:
        click.echo(
            f"Ping → '{name}' FAILED  (no reply within {timeout}s)",
            err=True,
        )
        raise SystemExit(1)
