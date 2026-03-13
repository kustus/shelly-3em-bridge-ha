"""UDP Broadcast Listener for the Shelly Pro 3EM Bridge.

Listens for Shelly RPC broadcast queries from Zendure/Marstek devices.
These devices send UDP broadcasts (EM.GetStatus) to standard Shelly ports
(1010, 2220) to discover and poll local Shelly EM devices.  This is the
PRIMARY mechanism for local (WiFi) power data exchange.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
from typing import Any

from .const import COMPACT_JSON, SHELLY_BROADCAST_PORTS

_LOGGER = logging.getLogger(__name__)


class UdpBroadcastListener:
    """Listens on standard Shelly broadcast ports and responds to RPC queries."""

    def __init__(self, coordinator: Any, ports: list[int] | None = None) -> None:
        self._coord = coordinator
        self._ports = ports or SHELLY_BROADCAST_PORTS
        self._transports: list[asyncio.DatagramTransport] = []

    async def async_start(self) -> None:
        """Bind UDP sockets on broadcast ports."""
        loop = asyncio.get_running_loop()
        for port in self._ports:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                sock.setblocking(False)
                sock.bind(("0.0.0.0", port))
                transport, _ = await loop.create_datagram_endpoint(
                    lambda: _BroadcastProtocol(self),
                    sock=sock,
                )
                self._transports.append(transport)
                _LOGGER.debug("UDP broadcast on port %d", port)
            except OSError as exc:
                _LOGGER.warning("Cannot bind broadcast port %d: %s", port, exc)

    async def async_stop(self) -> None:
        """Close all transports."""
        for t in self._transports:
            t.close()
        self._transports.clear()

    def handle_datagram(
        self, data: bytes, addr: tuple[str, int],
        transport: asyncio.DatagramTransport,
    ) -> None:
        """Process an incoming broadcast RPC request."""
        try:
            msg = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return

        method = msg.get("method", "")
        msg_id = msg.get("id")
        params = msg.get("params", {})

        # Validate params.id is int (some devices are strict about this)
        param_id = params.get("id")
        if method in ("EM.GetStatus", "EM1.GetStatus") and not isinstance(param_id, int):
            return

        result = self._dispatch(method, params)
        if result is None:
            return

        response = {
            "id": msg_id,
            "src": self._coord.device_id,
            "dst": "unknown",
            "result": result,
        }
        resp_data = json.dumps(response, **COMPACT_JSON).encode("utf-8")
        transport.sendto(resp_data, addr)

    def _dispatch(self, method: str, params: dict) -> dict | None:
        c = self._coord
        if method == "EM.GetStatus":
            return c.meter.to_em_status_udp()
        if method == "EM1.GetStatus":
            return c.meter.to_em1_status_udp()
        if method == "Shelly.GetDeviceInfo":
            return c.get_device_info_dict()
        if method == "EM.GetCTTypes":
            return {"ct_types": ["3x63A"]}
        return None


class _BroadcastProtocol(asyncio.DatagramProtocol):
    def __init__(self, owner: UdpBroadcastListener) -> None:
        self._owner = owner
        self._transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self._transport = transport

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        if self._transport:
            self._owner.handle_datagram(data, addr, self._transport)

    def error_received(self, exc: Exception) -> None:
        _LOGGER.debug("UDP broadcast error: %s", exc)
