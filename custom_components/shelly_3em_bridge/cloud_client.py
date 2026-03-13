"""Shelly Cloud WebSocket client for the Shelly Pro 3EM Bridge.

Connects to the Shelly Cloud via WebSocket (wss://server:6022/jrpc) and
impersonates the real Shelly Pro 3EM device using its JWT cloud key.

IMPORTANT: The real Shelly Pro 3EM must be powered off / disconnected from
WiFi while this client is running.
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
from typing import Any

import aiohttp

_LOGGER = logging.getLogger(__name__)

_PUSH_INTERVAL_S = 30
_RECONNECT_MIN_S = 5
_RECONNECT_MAX_S = 300


class ShellyCloudClient:
    """WebSocket client that connects to Shelly Cloud and pushes power data."""

    def __init__(
        self,
        coordinator: Any,
        cloud_server: str,
        cloud_key: str,
    ) -> None:
        self._coord = coordinator
        self._cloud_server = cloud_server
        self._cloud_key = cloud_key
        self._running = False
        self._task: asyncio.Task | None = None
        self._cloud_token: str | None = None
        self._ws: Any = None
        self._restart_requested = False
        self._reboot_done = False

    async def async_start(self) -> None:
        if not self._cloud_server or not self._cloud_key:
            return
        self._running = True
        self._task = asyncio.get_event_loop().create_task(
            self._run_loop(), name="shelly_bridge_cloud_client",
        )

    async def async_stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run_loop(self) -> None:
        backoff = _RECONNECT_MIN_S
        while self._running:
            try:
                await self._connect_and_run()
                backoff = _RECONNECT_MIN_S
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _LOGGER.debug("Cloud connection lost: %s – retry in %ds", exc, backoff)
            if not self._running:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _RECONNECT_MAX_S)

    async def _connect_and_run(self) -> None:
        url = f"wss://{self._cloud_server}"
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, ssl=ssl_ctx, heartbeat=30, receive_timeout=90) as ws:
                self._ws = ws
                self._restart_requested = False
                self._reboot_done = False

                # Announce presence
                await self._send_notification(ws, "NotifyFullStatus", {
                    **self._coord.get_full_status(),
                    "ts": time.time(),
                })

                push_task = asyncio.create_task(self._push_loop(ws))
                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._handle_message(ws, msg.data)
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE,
                                          aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                            break
                        if self._restart_requested:
                            self._restart_requested = False
                            break
                finally:
                    self._ws = None
                    push_task.cancel()
                    try:
                        await push_task
                    except asyncio.CancelledError:
                        pass

    async def _push_loop(self, ws) -> None:
        try:
            while self._running:
                await asyncio.sleep(_PUSH_INTERVAL_S)
                await self._send_notification(ws, "NotifyStatus", {
                    "ts": time.time(),
                    "em:0": self._coord.meter.to_em_status(),
                })
        except asyncio.CancelledError:
            raise

    async def _handle_message(self, ws, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        method = data.get("method", "")
        msg_id = data.get("id")
        params = data.get("params", {})

        if method == "Shelly.GetDeviceInfo":
            result = self._coord.get_device_info_dict(ident=params.get("ident", False))
            await self._send_response(ws, msg_id, result)

        elif method == "Sys.SetConfig":
            config = params.get("config", {})
            rpc_udp = config.get("rpc_udp")
            if rpc_udp:
                dst_addr = rpc_udp.get("dst_addr")
                listen_port = rpc_udp.get("listen_port")
                if dst_addr:
                    self._coord.rpc_udp_dst = dst_addr
                    self._coord.rpc_udp_port = listen_port
                    self._coord._cfg_rev += 1
                    if self._coord.udp_transport:
                        self._coord.udp_transport.set_destination(dst_addr)
                await self._send_response(ws, msg_id, {"restart_required": True})
                asyncio.get_running_loop().call_later(
                    5.0, self._trigger_restart_if_needed,
                )
            else:
                await self._send_response(ws, msg_id, {"restart_required": False})

        elif method == "Cloud.SetConfig":
            config = params.get("config", {})
            self._cloud_token = config.get("token")
            await self._send_response(ws, msg_id, {"restart_required": False})

        elif method == "Shelly.GetComponents":
            result = self._build_components()
            await self._send_response(ws, msg_id, result)

        elif method == "Shelly.GetStatus":
            await self._send_response(ws, msg_id, self._coord.get_full_status())

        elif method == "Shelly.GetConfig":
            await self._send_response(ws, msg_id, self._coord.get_full_config())

        elif method == "EM.GetStatus":
            await self._send_response(ws, msg_id, self._coord.meter.to_em_status())

        elif method == "EMData.GetStatus":
            await self._send_response(ws, msg_id, self._coord.meter.to_emdata_status())

        elif method == "EMData.GetData":
            result = {
                "id": params.get("id", 0),
                "data_blocks": [],
                **self._coord.meter.to_emdata_status(),
            }
            await self._send_response(ws, msg_id, result)

        elif method == "shc.channelready":
            await self._send_response(ws, msg_id, {})
            if self._coord.udp_transport and self._coord.rpc_udp_dst:
                self._coord.udp_transport._send_notify_status()

        elif method == "Shelly.Reboot":
            await self._send_response(ws, msg_id, {})
            self._coord.simulate_restart()
            self._trigger_restart()

        elif method == "Shelly.ListMethods":
            await self._send_response(ws, msg_id, {"methods": [
                "Shelly.GetDeviceInfo", "Shelly.GetStatus", "Shelly.GetConfig",
                "Shelly.ListMethods", "Shelly.GetComponents", "Shelly.Reboot",
                "Sys.GetStatus", "Sys.GetConfig", "Sys.SetConfig",
                "EM.GetStatus", "EM.GetConfig",
                "EMData.GetStatus", "EMData.GetData",
                "Cloud.GetStatus", "Cloud.GetConfig", "Cloud.SetConfig",
                "WiFi.GetStatus", "WiFi.GetConfig",
            ]})

        else:
            await self._send_response(ws, msg_id, {})

    def _trigger_restart(self) -> None:
        """Trigger a simulated restart (WS disconnect -> reconnect)."""
        self._reboot_done = True
        self._restart_requested = True
        if self._ws:
            asyncio.ensure_future(self._ws.close())

    def _trigger_restart_if_needed(self) -> None:
        if not getattr(self, '_reboot_done', False):
            self._trigger_restart()

    async def _send_response(self, ws, msg_id, result) -> None:
        frame = {
            "id": msg_id,
            "src": self._coord.device_id,
            "dst": "cloud",
            "result": result,
        }
        await ws.send_str(json.dumps(frame))

    async def _send_notification(self, ws, method: str, params: dict) -> None:
        frame = {
            "src": self._coord.device_id,
            "dst": "cloud",
            "method": method,
            "params": params,
        }
        await ws.send_str(json.dumps(frame))

    def _build_components(self) -> dict[str, Any]:
        c = self._coord
        return {
            "components": [
                {"key": "ble", "status": {}, "config": {"enable": True, "rpc": {"enable": True}}},
                {"key": "cloud", "status": {"connected": True}, "config": {"enable": True, "server": self._cloud_server}},
                {"key": "em:0", "status": c.meter.to_em_status(), "config": {"id": 0, "name": None, "ct_type": "3x63A", "reverse": {}}},
                {"key": "emdata:0", "status": c.meter.to_emdata_status(), "config": {"id": 0}},
                {"key": "eth", "status": {"ip": None}, "config": {"enable": True, "ipv4mode": "dhcp"}},
                {"key": "modbus", "status": {}, "config": {"enable": False}},
                {"key": "mqtt", "status": {"connected": False}, "config": {"enable": False, "topic_prefix": c.device_id}},
                {"key": "sys", "status": c.get_full_status().get("sys", {}), "config": c.get_full_config().get("sys", {})},
                {"key": "temperature:0", "status": {"id": 0, "tC": 38.5, "tF": 101.3}, "config": {"id": 0, "name": None, "report_thr_C": 5.0}},
                {"key": "wifi", "status": c.get_full_status().get("wifi", {}), "config": c.get_full_config().get("wifi", {})},
                {"key": "ws", "status": {"connected": False}, "config": {"enable": False, "server": None, "ssl_ca": "*"}},
            ],
            "cfg_rev": c._cfg_rev, "offset": 0, "total": 11,
        }
