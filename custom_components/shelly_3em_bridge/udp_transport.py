"""UDP RPC Transport for Cloud-configured push to Zendure Hyper.

The Cloud sends a Sys.SetConfig with rpc_udp parameters telling the Shelly:
  - listen_port: port to bind for incoming requests
  - dst_addr: "ip:port" of the Hyper to push NotifyStatus to

This class binds a UDP socket, responds to incoming RPC requests,
and periodically pushes NotifyStatus datagrams to the Hyper.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import time
from typing import Any

from .const import COMPACT_JSON

_LOGGER = logging.getLogger(__name__)


class UdpRpcTransport:
    """UDP-based RPC transport for local Shelly <-> Zendure Hyper communication."""

    PUSH_INTERVAL = 10  # seconds between periodic NotifyStatus pushes

    def __init__(
        self, coordinator: Any, listen_port: int, dst_addr: str | None = None,
    ) -> None:
        self._coord = coordinator
        self._listen_port = listen_port
        self._dst_host: str | None = None
        self._dst_port: int | None = None
        if dst_addr:
            self.set_destination(dst_addr)
        self._transport: asyncio.DatagramTransport | None = None
        self._push_task: asyncio.Task | None = None
        self._running = False
        self._last_push_ts: float = 0.0
        self._peer_id: str | None = None

    def set_destination(self, dst_addr: str) -> None:
        """Update the destination address (e.g. from Sys.SetConfig)."""
        host, port_s = dst_addr.rsplit(":", 1)
        self._dst_host = host
        self._dst_port = int(port_s)

    async def async_start(self) -> None:
        """Bind UDP socket with SO_REUSEADDR and start the push loop."""
        loop = asyncio.get_running_loop()
        self._running = True

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(False)
        sock.bind(("0.0.0.0", self._listen_port))

        transport, _ = await loop.create_datagram_endpoint(
            lambda: _UdpProtocol(self),
            sock=sock,
        )
        self._transport = transport
        self._push_task = asyncio.create_task(self._push_loop(), name="udp_push")

    async def async_stop(self) -> None:
        """Stop push loop and close socket."""
        self._running = False
        if self._push_task and not self._push_task.done():
            self._push_task.cancel()
            try:
                await self._push_task
            except asyncio.CancelledError:
                pass
        if self._transport:
            self._transport.close()
            self._transport = None

    # -- Outgoing push --

    async def _push_loop(self) -> None:
        """Periodically push NotifyStatus to the Hyper."""
        try:
            while self._running:
                await asyncio.sleep(self.PUSH_INTERVAL)
                self._send_notify_status()
        except asyncio.CancelledError:
            raise

    def _send_notify_status(self) -> None:
        """Build and send a NotifyStatus datagram to the Hyper."""
        if not self._transport or not self._dst_host:
            return
        frame = {
            "src": self._coord.device_id,
            "dst": "*",
            "method": "NotifyStatus",
            "params": {
                "ts": time.time(),
                "em:0": self._coord.meter.to_em_status_udp(),
            },
        }
        data = json.dumps(frame, **COMPACT_JSON).encode("utf-8")
        self._transport.sendto(data, (self._dst_host, self._dst_port))

    def send_notify_status_throttled(self) -> None:
        """Rate-limited push triggered by MQTT updates (max 1 per 2 sec)."""
        now = time.monotonic()
        if now - self._last_push_ts < 2.0:
            return
        self._last_push_ts = now
        self._send_notify_status()

    # -- Incoming RPC handling --

    def handle_datagram(self, data: bytes, addr: tuple[str, int]) -> None:
        """Called by _UdpProtocol when a datagram arrives."""
        try:
            msg = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return

        method = msg.get("method", "")
        msg_id = msg.get("id")
        params = msg.get("params", {})
        src = msg.get("src", "")

        if src and not self._peer_id:
            self._peer_id = src

        result = self._dispatch_udp_rpc(method, params)

        if msg_id is not None:
            response = {
                "id": msg_id,
                "src": self._coord.device_id,
                "dst": src or "unknown",
                "result": result,
            }
            resp_data = json.dumps(response, **COMPACT_JSON).encode("utf-8")
            if self._transport:
                self._transport.sendto(resp_data, addr)

    def _dispatch_udp_rpc(self, method: str, params: dict) -> Any:
        """Handle RPC methods that may arrive via UDP from the Hyper."""
        c = self._coord
        m = method.lower().replace(".", "_")

        handlers: dict[str, Any] = {
            "shelly_getdeviceinfo": lambda: c.get_device_info_dict(
                ident=params.get("ident", False)
            ),
            "shelly_getstatus": lambda: c.get_full_status(),
            "shelly_getconfig": lambda: c.get_full_config(),
            "em_getstatus": lambda: c.meter.to_em_status(),
            "emdata_getstatus": lambda: c.meter.to_emdata_status(),
            "emdata_getdata": lambda: {
                "id": 0, "data_blocks": [],
                **c.meter.to_emdata_status(),
            },
            "sys_getstatus": lambda: c.get_full_status().get("sys", {}),
            "sys_getconfig": lambda: c.get_full_config().get("sys", {}),
            "shelly_getcomponents": lambda: {
                "components": [
                    {"key": "em:0", "status": c.meter.to_em_status(),
                     "config": {"id": 0, "name": None, "ct_type": "3x63A", "reverse": {}}},
                    {"key": "emdata:0", "status": c.meter.to_emdata_status(),
                     "config": {"id": 0}},
                ],
                "cfg_rev": c._cfg_rev, "offset": 0, "total": 11,
            },
        }

        handler = handlers.get(m)
        if handler:
            return handler()
        return {}


class _UdpProtocol(asyncio.DatagramProtocol):
    """asyncio DatagramProtocol that delegates to UdpRpcTransport."""

    def __init__(self, transport_obj: UdpRpcTransport) -> None:
        self._owner = transport_obj

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self._owner.handle_datagram(data, addr)

    def error_received(self, exc: Exception) -> None:
        _LOGGER.debug("UDP error: %s", exc)

    def connection_lost(self, exc: Exception | None) -> None:
        pass
