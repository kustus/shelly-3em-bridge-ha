"""aiohttp HTTP + WebSocket server that simulates a Shelly Pro 3EM (Gen2).

Implements:
  - GET  /shelly              -> device identity
  - GET  /rpc/<Method>        -> RPC via query parameters
  - POST /rpc                 -> JSON-RPC 2.0 batch/single
  - POST /rpc/<Method>        -> RPC via URL + JSON body params
  - WS   /rpc                 -> WebSocket JSON-RPC channel
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import aiohttp
from aiohttp import web

from .const import SHELLY_FW_VERSION

_LOGGER = logging.getLogger(__name__)

_WS_QUEUE_SIZE = 64


class ShellyHttpServer:
    """aiohttp server emulating a Shelly Pro 3EM Gen2 RPC device."""

    def __init__(self, coordinator: Any, port: int) -> None:
        self._coord = coordinator
        self._port = port
        self._runner: web.AppRunner | None = None

    async def async_start(self) -> None:
        """Build the aiohttp app and start serving."""

        @web.middleware
        async def mongoose_header(request: web.Request, handler):
            try:
                resp = await handler(request)
                resp.headers["Server"] = "Mongoose/6.18"
                resp.headers["Access-Control-Allow-Origin"] = "*"
                return resp
            except web.HTTPException as exc:
                exc.headers["Server"] = "Mongoose/6.18"
                raise

        app = web.Application(middlewares=[mongoose_header])
        app.router.add_get("/shelly", self._handle_shelly)
        app.router.add_get("/rpc/{method}", self._handle_rpc_get)
        app.router.add_post("/rpc", self._handle_rpc_batch)
        app.router.add_post("/rpc/", self._handle_rpc_batch)
        app.router.add_post("/rpc/{method}", self._handle_rpc_post)
        app.router.add_get("/rpc", self._handle_ws)

        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self._port, reuse_address=True)
        await site.start()
        _LOGGER.debug("HTTP on port %d", self._port)

    async def async_stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None

    # -- HTTP handlers --

    async def _handle_shelly(self, request: web.Request) -> web.Response:
        return web.json_response(self._coord.get_device_info_dict())

    async def _handle_rpc_get(self, request: web.Request) -> web.Response:
        method = request.match_info["method"]
        params = dict(request.query)
        if "ident" in params:
            params["ident"] = params["ident"].lower() in ("true", "1")
        result = self._dispatch_rpc(method, params)
        return web.json_response(result)

    async def _handle_rpc_post(self, request: web.Request) -> web.Response:
        method = request.match_info["method"]
        try:
            params = await request.json()
        except Exception:
            params = {}
        result = self._dispatch_rpc(method, params)
        return web.json_response(result)

    async def _handle_rpc_batch(self, request: web.Request) -> web.Response:
        if request.headers.get("Upgrade", "").lower() == "websocket":
            return await self._handle_ws(request)

        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)

        if isinstance(body, list):
            results = []
            for item in body:
                method = item.get("method", "")
                params = item.get("params", {})
                msg_id = item.get("id")
                result = self._dispatch_rpc(method, params)
                results.append({"id": msg_id, "src": self._coord.device_id, "result": result})
            return web.json_response(results)

        method = body.get("method", "")
        params = body.get("params", {})
        msg_id = body.get("id")
        result = self._dispatch_rpc(method, params)
        return web.json_response({"id": msg_id, "src": self._coord.device_id, "result": result})

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)

        queue: asyncio.Queue = asyncio.Queue(maxsize=_WS_QUEUE_SIZE)
        await self._coord.register_ws(queue)

        full = {
            "src": self._coord.device_id,
            "method": "NotifyFullStatus",
            "params": self._coord.get_full_status(),
        }
        await ws.send_json(full)

        async def push_loop():
            try:
                while not ws.closed:
                    msg = await asyncio.wait_for(queue.get(), timeout=30)
                    await ws.send_json(msg)
            except (asyncio.TimeoutError, asyncio.CancelledError, ConnectionResetError):
                pass

        push_task = asyncio.create_task(push_loop())

        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except json.JSONDecodeError:
                        continue
                    method = data.get("method", "")
                    params = data.get("params", {})
                    msg_id = data.get("id")
                    result = self._dispatch_rpc(method, params)
                    await ws.send_json({
                        "id": msg_id,
                        "src": self._coord.device_id,
                        "dst": data.get("src", ""),
                        "result": result,
                    })
        finally:
            push_task.cancel()
            await self._coord.unregister_ws(queue)

        return ws

    # -- RPC dispatcher --

    def _dispatch_rpc(self, method: str, params: dict) -> Any:
        b = self._coord
        m = method.lower().replace(".", "_")

        dispatch = {
            "shelly_getdeviceinfo": lambda: b.get_device_info_dict(ident=params.get("ident", False)),
            "shelly_getstatus": lambda: b.get_full_status(),
            "shelly_getconfig": lambda: b.get_full_config(),
            "shelly_listmethods": lambda: {"methods": [
                "Shelly.GetDeviceInfo", "Shelly.GetStatus", "Shelly.GetConfig",
                "Shelly.ListMethods", "Shelly.CheckForUpdate", "Shelly.ListProfiles",
                "Shelly.GetComponents", "Shelly.Reboot",
                "Sys.GetStatus", "Sys.GetConfig", "Sys.SetConfig",
                "WiFi.GetStatus", "WiFi.GetConfig", "WiFi.SetConfig", "WiFi.Scan",
                "Eth.GetStatus", "Eth.GetConfig", "Eth.SetConfig",
                "BLE.GetStatus", "BLE.GetConfig", "BLE.SetConfig",
                "Cloud.GetStatus", "Cloud.GetConfig", "Cloud.SetConfig",
                "MQTT.GetStatus", "MQTT.GetConfig", "MQTT.SetConfig",
                "Modbus.GetStatus", "Modbus.GetConfig", "Modbus.SetConfig",
                "EM.GetStatus", "EM.GetConfig", "EM.SetConfig", "EM.ResetCounters",
                "EMData.GetStatus", "EMData.GetConfig", "EMData.SetConfig", "EMData.ResetCounters",
                "Temperature.GetStatus", "Temperature.GetConfig",
                "Schedule.List", "Schedule.Create", "Schedule.Update", "Schedule.Delete",
                "Webhook.List", "Webhook.ListSupported",
                "Script.List", "Script.GetCode", "Script.GetConfig", "Script.SetConfig",
                "Script.GetStatus", "Script.Create", "Script.Delete", "Script.PutCode",
                "Script.Start", "Script.Stop",
                "KVS.Get", "KVS.Set", "KVS.GetMany", "KVS.Delete", "KVS.List",
                "HTTP.GET",
            ]},
            "shelly_getcomponents": lambda: self._build_components(),
            "shelly_checkforupdate": lambda: {"stable": {"version": SHELLY_FW_VERSION}},
            "shelly_listprofiles": lambda: {"profiles": ["triphase", "monophase"]},
            "shelly_reboot": lambda: self._handle_reboot(),
            "shelly_update": lambda: None,
            "shelly_factoryreset": lambda: None,
            "shelly_resetwificred": lambda: None,
            "shelly_putuserca": lambda: None,
            "shelly_puttlsclientcert": lambda: None,
            "shelly_puttlsclientkey": lambda: None,
            "em_getstatus": lambda: b.meter.to_em_status(),
            "em_getconfig": lambda: b.get_full_config().get("em:0", {}),
            "em_setconfig": lambda: {"restart_required": False},
            "em_resetcounters": lambda: None,
            "emdata_getstatus": lambda: b.meter.to_emdata_status(),
            "emdata_getconfig": lambda: {"id": 0},
            "emdata_setconfig": lambda: {"restart_required": False},
            "emdata_resetcounters": lambda: None,
            "sys_getstatus": lambda: b.get_full_status().get("sys", {}),
            "sys_getconfig": lambda: b.get_full_config().get("sys", {}),
            "sys_setconfig": lambda: self._handle_sys_setconfig(params),
            "wifi_getstatus": lambda: b.get_full_status().get("wifi", {}),
            "wifi_getconfig": lambda: b.get_full_config().get("wifi", {}),
            "wifi_setconfig": lambda: {"restart_required": False},
            "wifi_scan": lambda: {"results": []},
            "eth_getstatus": lambda: {"ip": b.host_ip, "ip6": None},
            "eth_getconfig": lambda: b.get_full_config().get("eth", {}),
            "eth_setconfig": lambda: {"restart_required": False},
            "ble_getstatus": lambda: {},
            "ble_getconfig": lambda: {"enable": True, "rpc": {"enable": True}},
            "ble_setconfig": lambda: {"restart_required": False},
            "cloud_getstatus": lambda: {"connected": True},
            "cloud_getconfig": lambda: b.get_full_config().get("cloud", {}),
            "cloud_setconfig": lambda: {"restart_required": False},
            "mqtt_getstatus": lambda: {"connected": False},
            "mqtt_getconfig": lambda: b.get_full_config().get("mqtt", {}),
            "mqtt_setconfig": lambda: {"restart_required": False},
            "modbus_getstatus": lambda: {},
            "modbus_getconfig": lambda: {"enable": False},
            "modbus_setconfig": lambda: {"restart_required": False},
            "temperature_getstatus": lambda: {"id": 0, "tC": 38.5, "tF": 101.3},
            "temperature_getconfig": lambda: {"id": 0, "name": None, "report_thr_C": 5.0},
            "kvs_getmany": lambda: {"items": {}},
            "kvs_get": lambda: None,
            "kvs_list": lambda: {"keys": {}, "rev": 0},
            "schedule_list": lambda: {"jobs": []},
            "webhook_list": lambda: {"hooks": [], "rev": 0},
            "webhook_listsupported": lambda: {"hook_types": []},
            "script_list": lambda: {"scripts": []},
            "script_getcode": lambda: {"data": ""},
        }

        handler = dispatch.get(m)
        if handler:
            return handler()

        _LOGGER.debug("Unhandled RPC: %s", method)
        return {}

    def _handle_sys_setconfig(self, params: dict) -> dict:
        """Process Sys.SetConfig – especially rpc_udp for local Hyper comm."""
        config = params.get("config", {})
        rpc_udp = config.get("rpc_udp")
        if rpc_udp:
            dst_addr = rpc_udp.get("dst_addr")
            listen_port = rpc_udp.get("listen_port")
            if dst_addr:
                self._coord.rpc_udp_dst = dst_addr
                self._coord.rpc_udp_port = listen_port
                if self._coord.udp_transport:
                    self._coord.udp_transport.set_destination(dst_addr)
                    self._coord.udp_transport._send_notify_status()
        return {"restart_required": False}

    def _handle_reboot(self) -> None:
        """Simulate a device reboot."""
        self._coord.simulate_restart()
        return None

    def _build_components(self) -> dict:
        b = self._coord
        return {
            "components": [
                {"key": "ble", "status": {}, "config": {"enable": True, "rpc": {"enable": True}}},
                {"key": "cloud", "status": {"connected": True}, "config": {"enable": True, "server": b.get_full_config().get("cloud", {}).get("server", "")}},
                {"key": "em:0", "status": b.meter.to_em_status(), "config": {"id": 0, "name": None, "blink_mode_selector": "active_energy", "phase_selector": "all", "monitor_phase_sequence": False, "ct_type": "3x63A", "reverse": {}}},
                {"key": "emdata:0", "status": b.meter.to_emdata_status(), "config": {"id": 0}},
                {"key": "eth", "status": {"ip": None, "ip6": None}, "config": {"enable": True, "ipv4mode": "dhcp"}},
                {"key": "modbus", "status": {}, "config": {"enable": False}},
                {"key": "mqtt", "status": {"connected": False}, "config": {"enable": False, "topic_prefix": b.device_id}},
                {"key": "sys", "status": b.get_full_status().get("sys", {}), "config": b.get_full_config().get("sys", {})},
                {"key": "temperature:0", "status": {"id": 0, "tC": 38.5, "tF": 101.3}, "config": {"id": 0, "name": None, "report_thr_C": 5.0}},
                {"key": "wifi", "status": b.get_full_status().get("wifi", {}), "config": b.get_full_config().get("wifi", {})},
                {"key": "ws", "status": {"connected": False}, "config": {"enable": False, "server": None, "ssl_ca": "*"}},
            ],
            "cfg_rev": b._cfg_rev,
            "offset": 0,
            "total": 11,
        }
