"""mDNS / Zeroconf service registration for the Shelly Pro 3EM Bridge.

Root cause of "device found but inactive" in Shelly App scan:
─────────────────────────────────────────────────────────────
The Shelly App (and aioshelly) ignores the port number in the mDNS record
and always connects to port 80.  Any other port means the TCP connection
hits nothing or the wrong service → device appears in the scan list but is
shown as "inactive / unavailable".

Gen2 mDNS requirements (from https://shelly-api-docs.shelly.cloud/gen2/General/mDNS):
  • TWO service types must be registered, BOTH on port 80:
      1. _shelly._tcp.local.  – Shelly-specific (Gen2+)
      2. _http._tcp.local.    – generic web server (also used for Gen1)
  • TXT records must include: gen=2, app=Pro3EM, ver=<fw>
  • The mDNS server hostname format used by real devices:
      ShellyPro3EM-{UPPERCASE_MAC_NO_COLONS}.local.
    (CamelCase product name + dash + uppercase hex MAC)
  • Service instance name (same pattern):
      ShellyPro3EM-{UPPERCASE_MAC}
  • discoverable=true is expected by the Shelly App

Previous bugs fixed in this version:
  1. Only _shelly._tcp was registered → _http._tcp missing
  2. Port 7580 was advertised → app tried port 80 → no response
  3. Hostname used lowercase device_id format → mismatch vs real devices
  4. Private Zeroconf() instance → HA warning; now uses async_get_instance()
"""

from __future__ import annotations

import logging
import socket
from typing import Any

from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

# Both service types required for full Shelly Gen2 compatibility
_SERVICE_TYPE_SHELLY = "_shelly._tcp.local."
_SERVICE_TYPE_HTTP   = "_http._tcp.local."


class MdnsService:
    """Registers and unregisters both Shelly Gen2 mDNS service records.

    A real Shelly Pro 3EM advertises:
      ShellyPro3EM-AABBCCDDEEFF._shelly._tcp.local.  port=80
      ShellyPro3EM-AABBCCDDEEFF._http._tcp.local.    port=80
    Both records point to the same IP and carry the same TXT data.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        coordinator: Any,
        port: int,
    ) -> None:
        self._hass = hass
        self._coord = coordinator
        self._port = port
        self._host_ip: str | None = None
        # Holds both ServiceInfo objects for clean unregister
        self._registrations: list[Any] = []

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    @property
    def host_ip(self) -> str | None:
        """Return the detected host IP address (available after register)."""
        return self._host_ip

    async def async_register(self) -> None:
        """Register both mDNS service types using HA's shared Zeroconf."""
        try:
            from homeassistant.components.zeroconf import async_get_instance
            from zeroconf import ServiceInfo
        except ImportError as exc:
            _LOGGER.error("Cannot import Zeroconf dependencies: %s", exc)
            return

        host_ip = await self._hass.async_add_executor_job(self._get_host_ip)
        self._host_ip = host_ip
        if not host_ip:
            _LOGGER.warning(
                "Could not determine host IP address; mDNS registration skipped"
            )
            return

        zeroconf = await async_get_instance(self._hass)

        # ── Shared identity values ────────────────────────────────────────────
        mac_upper = self._coord.mac_clean          # e.g. "AABBCCDDEEFF"

        # CamelCase instance name matching real Shelly firmware convention
        # e.g. "ShellyPro3EM63-AC15186C51D0" (63 matches the model suffix)
        instance_name = f"ShellyPro3EM63-{mac_upper}"

        # mDNS server hostname (A-record): "ShellyPro3EM-AABBCCDDEEFF.local."
        mdns_hostname = f"{instance_name}.local."

        addr_bytes = socket.inet_aton(host_ip)

        # TXT properties identical to what a real Shelly Pro 3EM broadcasts
        properties = {
            "gen":          "2",
            "app":          "Pro3EM",
            "ver":          "1.7.1",
            "id":           self._coord.device_id,   # shellypro3em-aabbccddeeff
            "discoverable": "true",
        }

        service_configs = [
            (_SERVICE_TYPE_SHELLY, f"{instance_name}.{_SERVICE_TYPE_SHELLY}"),
            (_SERVICE_TYPE_HTTP,   f"{instance_name}.{_SERVICE_TYPE_HTTP}"),
        ]

        for svc_type, svc_name in service_configs:
            info = ServiceInfo(
                type_=svc_type,
                name=svc_name,
                addresses=[addr_bytes],
                port=self._port,        # Must be 80; Shelly App ignores mDNS port
                properties=properties,
                server=mdns_hostname,
            )
            try:
                await self._hass.async_add_executor_job(
                    zeroconf.register_service, info
                )
                self._registrations.append(info)
            except Exception as exc:
                _LOGGER.warning(
                    "mDNS registration failed for %s: %s", svc_type, exc
                )

    async def async_unregister(self) -> None:
        """Unregister all mDNS service records on unload."""
        if not self._registrations:
            return
        try:
            from homeassistant.components.zeroconf import async_get_instance
            zeroconf = await async_get_instance(self._hass)
            for info in self._registrations:
                try:
                    await self._hass.async_add_executor_job(
                        zeroconf.unregister_service, info
                    )
                    _LOGGER.debug("mDNS unregistered: %s", info.name)
                except Exception as exc:
                    _LOGGER.debug("mDNS unregister minor error: %s", exc)
        except Exception as exc:
            _LOGGER.debug("mDNS unregister error (non-critical): %s", exc)
        finally:
            self._registrations.clear()

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _get_host_ip() -> str | None:
        """Resolve the LAN IP of the machine running HA (executor-safe)."""
        try:
            # UDP trick: connect to external IP to find outgoing interface.
            # No packets are actually sent.
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except OSError as exc:
            _LOGGER.debug("Primary IP detection failed: %s", exc)
        try:
            return socket.gethostbyname(socket.gethostname())
        except OSError:
            return None
