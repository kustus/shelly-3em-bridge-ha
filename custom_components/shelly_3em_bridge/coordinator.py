"""Coordinator for the Shelly Pro 3EM Bridge.

Lessons from previous log analysis:
  1. paho-mqtt _on_message ran after asyncio loop was closed → guard with
     _shutdown_event + try/except RuntimeError.
  2. Multiple MQTT client instances with random IDs → use deterministic
     MAC-derived client ID so the broker evicts the stale connection.
  3. _async_accumulate_energy task blocked HA shutdown → store task ref,
     cancel explicitly, listen to EVENT_HOMEASSISTANT_STOP.
  4. Inconsistent MAC addresses produced duplicate Shelly entries → all
     responses derive IDs from a single CONF_DEVICE_MAC config value.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import timedelta
from typing import Any

from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import (
    CONF_CLOUD_KEY,
    CONF_DEVICE_MAC,
    CONF_MQTT_BROKER,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_TOPIC,
    CONF_MQTT_USERNAME,
    DOMAIN,
    SHELLY_APP,
    SHELLY_DEVICE_NAME,
    SHELLY_FW_ID,
    SHELLY_FW_VERSION,
    SHELLY_GEN,
    SHELLY_ID_PREFIX,
    SHELLY_MODEL,
    SHELLY_PROFILE,
    SIMULATED_FREQUENCY_HZ,
    SIMULATED_VOLTAGE_V,
)

_LOGGER = logging.getLogger(__name__)

# Fallback polling interval – normally data arrives via MQTT push
_FALLBACK_POLL_INTERVAL = timedelta(minutes=1)
# Energy accumulation interval in seconds
_ENERGY_INTERVAL_S = 10


def _decimal_enforce(val: float) -> float:
    """Ensure float has a decimal point in JSON by nudging integers by 0.001.

    Some devices (Marstek, Zendure) parse power values as strings and need a
    decimal point to be present.  E.g. 0 -> 0.001, 100.0 -> 100.001.
    """
    if abs(val) < 0.1:
        return 0.001
    if val == int(val):
        return val + 0.001
    return val


class ShellyMeterData:
    """In-memory representation of a Shelly Pro 3EM meter reading.

    All three phases are stored but the bridge currently maps the single
    MQTT watt value onto phase A; phases B and C remain at zero.
    """

    def __init__(self) -> None:
        self.power_w: float = 0.0          # Active power (W)
        self.voltage_v: float = SIMULATED_VOLTAGE_V
        self.frequency_hz: float = SIMULATED_FREQUENCY_HZ
        self.total_energy_wh: float = 0.0  # Accumulated active energy (Wh)
        self.last_update: float = 0.0      # monotonic timestamp

    @property
    def current_a(self) -> float:
        """Derive current from power and voltage (Ohm's law approximation)."""
        if self.voltage_v > 0:
            return round(abs(self.power_w) / self.voltage_v, 3)
        return 0.0

    # ── Shelly Gen2 response shapes ──────────────────────────────────────────

    def to_em_status(self) -> dict[str, Any]:
        """Return EM.GetStatus payload in Shelly Pro 3EM format."""
        pf = 0.997 if self.power_w != 0 else 0.0
        aprt = round(abs(self.power_w) / pf, 1) if pf else 0.0
        return {
            "id": 0,
            "a_current": self.current_a,
            "a_voltage": round(self.voltage_v, 1),
            "a_act_power": round(self.power_w, 1),
            "a_aprt_power": aprt,
            "a_pf": pf,
            "a_freq": round(self.frequency_hz, 2),
            "b_current": 0.0,
            "b_voltage": round(self.voltage_v, 1),
            "b_act_power": 0.0,
            "b_aprt_power": 0.0,
            "b_pf": 0.0,
            "b_freq": round(self.frequency_hz, 2),
            "c_current": 0.0,
            "c_voltage": round(self.voltage_v, 1),
            "c_act_power": 0.0,
            "c_aprt_power": 0.0,
            "c_pf": 0.0,
            "c_freq": round(self.frequency_hz, 2),
            "n_current": None,
            "total_current": self.current_a,
            "total_act_power": round(self.power_w, 1),
            "total_aprt_power": aprt,
            "user_calibrated_phase": [],
        }

    def to_em_status_udp(self) -> dict[str, Any]:
        """Minimal EM status for UDP EM.GetStatus responses (only power fields)."""
        return {
            "id": 0,
            "a_act_power": _decimal_enforce(round(self.power_w, 1)),
            "b_act_power": _decimal_enforce(0.0),
            "c_act_power": _decimal_enforce(0.0),
            "total_act_power": _decimal_enforce(round(self.power_w, 1)),
        }

    def to_em1_status_udp(self) -> dict[str, Any]:
        """Minimal EM1 status for UDP EM1.GetStatus responses."""
        return {
            "id": 0,
            "act_power": _decimal_enforce(round(self.power_w, 1)),
        }

    def to_emdata_status(self) -> dict[str, Any]:
        """Return EMData.GetStatus payload in Shelly Pro 3EM format (Wh)."""
        total_wh = round(self.total_energy_wh, 3)
        return {
            "id": 0,
            "a_total_act_energy": total_wh,
            "a_total_act_ret_energy": 0.0,
            "b_total_act_energy": 0.0,
            "b_total_act_ret_energy": 0.0,
            "c_total_act_energy": 0.0,
            "c_total_act_ret_energy": 0.0,
            "total_act": total_wh,
            "total_act_ret": 0.0,
        }



class ShellyBridgeCoordinator(DataUpdateCoordinator):
    """Central coordinator: subscribes to an external MQTT broker, provides
    data to HA sensors, and pushes updates to WebSocket clients.

    Lifecycle: created in async_setup_entry → async_start() →
               async_shutdown() in async_unload_entry and HA stop handler.
    """

    def __init__(self, hass: HomeAssistant, config_entry) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=_FALLBACK_POLL_INTERVAL,
        )
        self._config = config_entry.data
        self.meter = ShellyMeterData()

        # paho-mqtt instance – created in an executor to avoid blocking
        self._mqtt_client: Any = None  # paho.mqtt.client.Client

        # Threading event signals paho callbacks to stop touching the loop
        self._shutdown_event = threading.Event()

        # asyncio task for background energy accumulation
        self._energy_task: asyncio.Task | None = None

        # HA stop-event subscription canceller
        self._unsub_stop: Any = None

        # Set of (queue, loop) tuples for connected WebSocket sessions;
        # managed by http_server.py via register_ws / unregister_ws.
        self._ws_queues: set[asyncio.Queue] = set()
        self._ws_lock = asyncio.Lock()

        # Host IP – set during setup by __init__.py after mDNS resolves it
        self.host_ip: str | None = None

        # UDP RPC config – set by Sys.SetConfig from cloud
        self.rpc_udp_dst: str | None = None
        self.rpc_udp_port: int | None = None
        self.udp_transport: Any = None  # UdpRpcTransport instance

        # Simulated restart tracking
        self._boot_time: float = time.monotonic()
        self._cfg_rev: int = 12

        # WiFi SSID from config
        self.wifi_ssid: str = "J-A-Castle"

        # Cloud key for ident responses
        self._cloud_key: str = self._config.get(CONF_CLOUD_KEY, "")

    # ── Public lifecycle ──────────────────────────────────────────────────────

    async def async_start(self) -> None:
        """Start MQTT client and background tasks."""
        # Register an HA-stop listener BEFORE starting paho so we can
        # signal the shutdown event before the asyncio loop closes.
        self._unsub_stop = self.hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STOP,
            self._async_on_ha_stop,
        )

        # Connect paho-mqtt in a thread (blocking socket operations)
        await self.hass.async_add_executor_job(self._setup_mqtt)

        # Start energy accumulation as a named, tracked task
        self._energy_task = self.hass.loop.create_task(
            self._async_accumulate_energy(),
            name=f"{DOMAIN}_energy_accumulation",
        )
        _LOGGER.debug("ShellyBridgeCoordinator started")

    async def async_shutdown(self) -> None:
        """Gracefully shut down the coordinator.

        Called from async_unload_entry (manual reload/remove) and
        indirectly from _async_on_ha_stop (HA shutdown).
        """
        _LOGGER.debug("ShellyBridgeCoordinator shutting down")

        # 1. Signal all paho threads to stop touching the event loop
        self._shutdown_event.set()

        # 2. Cancel background energy task
        if self._energy_task and not self._energy_task.done():
            self._energy_task.cancel()
            try:
                await self._energy_task
            except asyncio.CancelledError:
                pass
            self._energy_task = None

        # 3. Unsubscribe HA stop listener (if unload happens before stop)
        if self._unsub_stop:
            self._unsub_stop()
            self._unsub_stop = None

        # 4. Disconnect MQTT in executor (blocks)
        if self._mqtt_client is not None:
            await self.hass.async_add_executor_job(self._disconnect_mqtt)
            self._mqtt_client = None

    # ── HA stop handler ───────────────────────────────────────────────────────

    @callback
    def _async_on_ha_stop(self, _event: Any) -> None:
        """Called synchronously when HA emits EVENT_HOMEASSISTANT_STOP.

        We signal the shutdown event here – before the loop closes –
        so paho's _on_message callbacks will not call call_soon_threadsafe
        on a closed loop.
        """
        _LOGGER.debug("HA stop event received, signalling paho shutdown")
        self._shutdown_event.set()
        # Best-effort immediate disconnect to free broker resources
        if self._mqtt_client is not None:
            try:
                self._mqtt_client.loop_stop()
                self._mqtt_client.disconnect()
            except Exception:  # noqa: BLE001
                pass

    # ── paho-mqtt setup (executor) ────────────────────────────────────────────

    def _setup_mqtt(self) -> None:
        """Create and connect the paho MQTT client.

        Runs in an executor thread.  Uses a deterministic client ID derived
        from the configured MAC address so that the MQTT broker evicts any
        stale connection from a previous HA run – preventing the duplicate
        client problem seen in earlier log entries.
        """
        import paho.mqtt.client as mqtt  # noqa: PLC0415

        broker = self._config[CONF_MQTT_BROKER]
        port = int(self._config[CONF_MQTT_PORT])
        topic = self._config[CONF_MQTT_TOPIC]
        username = self._config.get(CONF_MQTT_USERNAME) or None
        password = self._config.get(CONF_MQTT_PASSWORD) or None

        # Deterministic, collision-free client ID (same across restarts)
        mac_hex = self._config[CONF_DEVICE_MAC].replace(":", "").lower()
        client_id = f"ha_shelly_bridge_{mac_hex}"

        # paho 2.x requires CallbackAPIVersion; fall back gracefully for 1.x
        try:
            from paho.mqtt.client import CallbackAPIVersion  # noqa: PLC0415
            client = mqtt.Client(
                callback_api_version=CallbackAPIVersion.VERSION1,
                client_id=client_id,
                clean_session=True,
            )
        except (ImportError, TypeError):
            client = mqtt.Client(client_id=client_id, clean_session=True)

        if username:
            client.username_pw_set(username, password)

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message

        try:
            client.connect(broker, port, keepalive=60)
            client.loop_start()
            self._mqtt_client = client
            _LOGGER.debug("MQTT connected to %s:%d", broker, port)
        except Exception as exc:  # noqa: BLE001
            _LOGGER.error(
                "Failed to connect to MQTT broker %s:%d – %s", broker, port, exc
            )

    def _disconnect_mqtt(self) -> None:
        """Disconnect paho MQTT client. Runs in an executor thread."""
        if self._mqtt_client is None:
            return
        try:
            self._mqtt_client.loop_stop()
            self._mqtt_client.disconnect()
            _LOGGER.debug("MQTT client disconnected cleanly")
        except Exception as exc:  # noqa: BLE001
            _LOGGER.debug("Minor error during MQTT disconnect: %s", exc)

    # ── paho callbacks (execute in paho's internal thread) ───────────────────

    def _on_connect(self, client: Any, userdata: Any, flags: Any, rc: int) -> None:
        """Subscribe to the configured topic after a successful connect."""
        if rc == 0:
            topic = self._config[CONF_MQTT_TOPIC]
            client.subscribe(topic, qos=0)
            _LOGGER.debug("MQTT subscribed to '%s'", topic)
        else:
            _LOGGER.warning("MQTT connection refused (rc=%d)", rc)

    def _on_disconnect(
        self, client: Any, userdata: Any, rc: int
    ) -> None:
        """Log unexpected disconnects (paho reconnects automatically)."""
        if self._shutdown_event.is_set():
            _LOGGER.debug("MQTT disconnected during shutdown (rc=%d)", rc)
        else:
            _LOGGER.warning(
                "MQTT unexpectedly disconnected (rc=%d); will retry", rc
            )

    def _on_message(
        self, client: Any, userdata: Any, msg: Any
    ) -> None:
        """Receive a power reading and forward it to the asyncio loop.

        KEY FIX: Check _shutdown_event and guard call_soon_threadsafe with
        try/except RuntimeError to prevent 'Event loop is closed' crashes
        that occurred in previous versions when HA shut down while paho's
        background thread was still active.
        """
        if self._shutdown_event.is_set():
            return

        try:
            payload = msg.payload.decode("utf-8").strip()
            value = float(payload)
        except (ValueError, UnicodeDecodeError) as exc:
            _LOGGER.warning(
                "Ignoring malformed MQTT payload on '%s': %r – %s",
                msg.topic,
                msg.payload,
                exc,
            )
            return

        try:
            if not self.hass.loop.is_closed():
                self.hass.loop.call_soon_threadsafe(
                    self._async_handle_power_update, value
                )
        except RuntimeError:
            # Loop already closed during HA shutdown – safe to ignore
            pass

    # ── asyncio handlers (execute in event loop thread) ──────────────────────

    @callback
    def _async_handle_power_update(self, power_w: float) -> None:
        """Update meter data and push to coordinator + WebSocket clients."""
        self.meter.power_w = power_w
        self.meter.last_update = time.monotonic()
        # Update coordinator so HA sensor entities receive fresh state
        self.async_set_updated_data(self._build_ha_data())
        # Push Shelly-protocol notification to all WebSocket sessions
        self.hass.loop.create_task(
            self._async_push_ws_notification(),
            name=f"{DOMAIN}_ws_push",
        )
        # Also push via UDP if transport is active (rate-limited)
        if self.udp_transport:
            self.udp_transport.send_notify_status_throttled()

    def _build_ha_data(self) -> dict[str, Any]:
        """Assemble the data dict exposed to HA sensor entities."""
        return {
            "power_w": self.meter.power_w,
            "voltage_v": self.meter.voltage_v,
            "current_a": self.meter.current_a,
            "frequency_hz": self.meter.frequency_hz,
            "total_energy_wh": self.meter.total_energy_wh,
        }

    # ── WebSocket push notification ───────────────────────────────────────────

    async def register_ws(self, queue: asyncio.Queue) -> None:
        """Register a WebSocket session for push notifications."""
        async with self._ws_lock:
            self._ws_queues.add(queue)

    async def unregister_ws(self, queue: asyncio.Queue) -> None:
        """Remove a WebSocket session."""
        async with self._ws_lock:
            self._ws_queues.discard(queue)

    async def _async_push_ws_notification(self) -> None:
        """Enqueue a NotifyStatus frame for all active WebSocket clients."""
        if not self._ws_queues:
            return
        payload = {
            "src": self.device_id,
            "method": "NotifyStatus",
            "params": {
                "ts": time.time(),
                "em:0": self.meter.to_em_status(),
            },
        }
        async with self._ws_lock:
            dead: set[asyncio.Queue] = set()
            for q in self._ws_queues:
                try:
                    q.put_nowait(payload)
                except asyncio.QueueFull:
                    _LOGGER.debug("WS queue full, dropping notification")
                except Exception:  # noqa: BLE001
                    dead.add(q)
            self._ws_queues -= dead

    # ── Energy accumulation background task ──────────────────────────────────

    async def _async_accumulate_energy(self) -> None:
        """Integrate power (W) over time to compute energy (Wh).

        KEY FIX: asyncio.CancelledError is re-raised so the task terminates
        cleanly when async_shutdown() cancels it during HA shutdown.
        """
        _LOGGER.debug("Energy accumulation task started (interval=%ds)", _ENERGY_INTERVAL_S)
        try:
            while True:
                await asyncio.sleep(_ENERGY_INTERVAL_S)
                # Wh = W × (seconds / 3600)
                if self.meter.power_w > 0:
                    self.meter.total_energy_wh += (
                        self.meter.power_w * _ENERGY_INTERVAL_S / 3600.0
                    )
        except asyncio.CancelledError:
            _LOGGER.debug("Energy accumulation task cancelled cleanly")
            raise  # Must re-raise so asyncio marks the task as cancelled

    # ── Fallback polling (DataUpdateCoordinator) ──────────────────────────────

    async def _async_update_data(self) -> dict[str, Any]:
        """Fallback: called by DataUpdateCoordinator on schedule.

        Normally data arrives via MQTT push; this just returns current state.
        """
        return self._build_ha_data()

    # ── Shelly device identity helpers ───────────────────────────────────────

    @property
    def mac_clean(self) -> str:
        """MAC without colons, UPPERCASE. e.g. 'AABBCCDDEEFF'."""
        return self._config[CONF_DEVICE_MAC].replace(":", "").upper()

    @property
    def mac_colon(self) -> str:
        """MAC with colons, UPPERCASE. e.g. 'AA:BB:CC:DD:EE:FF'."""
        m = self.mac_clean
        return ":".join(m[i : i + 2] for i in range(0, 12, 2))

    @property
    def device_id(self) -> str:
        """Shelly device ID: 'shellypro3em63-aabbccddeeff' (lowercase)."""
        return f"{SHELLY_ID_PREFIX}-{self.mac_clean.lower()}"

    def simulate_restart(self) -> None:
        """Reset uptime counter to simulate a device restart."""
        self._boot_time = time.monotonic()

    def get_device_info_dict(self, ident: bool = False) -> dict[str, Any]:
        """Return /shelly and Shelly.GetDeviceInfo response body."""
        info = {
            "name": SHELLY_DEVICE_NAME,
            "id": self.device_id,
            "mac": self.mac_clean,
            "slot": 0,
            "model": SHELLY_MODEL,
            "gen": SHELLY_GEN,
            "fw_id": SHELLY_FW_ID,
            "ver": SHELLY_FW_VERSION,
            "app": SHELLY_APP,
            "profile": SHELLY_PROFILE,
            "auth_en": False,
            "auth_domain": None,
        }
        if ident and self._cloud_key:
            info["key"] = self._cloud_key
            info["batch"] = "2431-Broadwell"
            info["fw_sbits"] = "04"
        return info

    def get_full_status(self) -> dict[str, Any]:
        """Return Shelly.GetStatus response body."""
        ts = int(time.time())
        uptime = int(time.monotonic() - self._boot_time)
        return {
            "ble": {},
            "cloud": {"connected": True},
            "em:0": self.meter.to_em_status(),
            "emdata:0": self.meter.to_emdata_status(),
            "eth": {"ip": self.host_ip},
            "modbus": {},
            "mqtt": {"connected": False},
            "sys": {
                "mac": self.mac_clean,
                "restart_required": False,
                "time": time.strftime("%H:%M"),
                "unixtime": ts,
                "last_sync_ts": ts - 60,
                "uptime": uptime,
                "ram_size": 255736, "ram_free": 76476,
                "fs_size": 524288, "fs_free": 188416,
                "cfg_rev": self._cfg_rev, "kvs_rev": 0,
                "schedule_rev": 0, "webhook_rev": 0,
                "available_updates": {},
                "wakeup_reason": {"boot": "poweron", "cause": ""},
            },
            "temperature:0": {"id": 0, "tC": 38.5, "tF": 101.3},
            "wifi": {
                "sta_ip": self.host_ip,
                "status": "got ip",
                "ssid": self.wifi_ssid,
                "rssi": -55,
            },
            "ws": {"connected": False},
        }

    def get_full_config(self) -> dict[str, Any]:
        """Return Shelly.GetConfig response body matching real Pro 3EM."""
        return {
            "ble": {"enable": True, "rpc": {"enable": True}},
            "cloud": {
                "enable": True,
                "server": "shelly-171-eu.shelly.cloud:6022/jrpc",
            },
            "em:0": {
                "id": 0, "name": None,
                "blink_mode_selector": "active_energy",
                "phase_selector": "all",
                "monitor_phase_sequence": False,
                "ct_type": "3x63A", "reverse": {},
            },
            "emdata:0": {"id": 0},
            "eth": {
                "enable": True, "ipv4mode": "dhcp",
                "ip": None, "netmask": None, "gw": None, "nameserver": None,
            },
            "modbus": {"enable": False},
            "mqtt": {
                "enable": False, "server": None, "user": None,
                "ssl_ca": None, "topic_prefix": self.device_id,
                "rpc_ntf": True, "status_ntf": False,
                "use_client_cert": False, "enable_rpc": True,
                "enable_control": True,
            },
            "sys": {
                "device": {
                    "name": SHELLY_DEVICE_NAME, "eco_mode": False,
                    "mac": self.mac_clean, "fw_id": SHELLY_FW_ID,
                    "profile": SHELLY_PROFILE, "discoverable": True,
                },
                "location": {"tz": "Europe/Berlin", "lat": None, "lon": None},
                "debug": {
                    "mqtt": {"enable": False},
                    "websocket": {"enable": False},
                    "udp": {"addr": None},
                },
                "ui_data": {},
                "rpc_udp": {
                    "dst_addr": self.rpc_udp_dst,
                    "listen_port": self.rpc_udp_port,
                },
                "sntp": {"server": "time.google.com"},
                "cfg_rev": self._cfg_rev,
            },
            "wifi": {
                "ap": {
                    "ssid": f"ShellyPro3EM63-{self.mac_clean}",
                    "is_open": True, "enable": False,
                    "range_extender": {"enable": False},
                },
                "sta": {
                    "ssid": self.wifi_ssid, "is_open": False,
                    "enable": True, "ipv4mode": "dhcp",
                    "ip": None, "netmask": None, "gw": None, "nameserver": None,
                },
                "sta1": {
                    "ssid": None, "is_open": True,
                    "enable": False, "ipv4mode": "dhcp",
                    "ip": None, "netmask": None, "gw": None, "nameserver": None,
                },
                "roam": {"rssi_thr": -80, "interval": 60},
            },
        }
