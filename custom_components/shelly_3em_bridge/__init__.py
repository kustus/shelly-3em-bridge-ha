"""Shelly Pro 3EM Bridge – Home Assistant Custom Integration.

This integration:
  1. Subscribes to a single MQTT topic and reads power consumption in Watts.
  2. Exposes HA sensor entities for power, voltage, current, frequency, energy.
  3. Simulates a complete Shelly Pro 3EM Gen2 device on the LAN:
       - aiohttp HTTP/WebSocket server (RPC protocol)
       - mDNS service registration
       - UDP broadcast listener (ports 1010/2220 for local Zendure/Marstek)
       - UDP RPC transport (port 8006 for Cloud-configured push)
  4. (Optional) Connects to Shelly Cloud via WebSocket for Zendure Hyper 2000.
"""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import (
    CONF_CLOUD_ENABLED,
    CONF_CLOUD_KEY,
    CONF_CLOUD_SERVER,
    CONF_HTTP_PORT,
    DATA_CLOUD_CLIENT,
    DATA_COORDINATOR,
    DATA_HTTP_SERVER,
    DATA_MDNS,
    DATA_UDP_LISTENER,
    DATA_UDP_TRANSPORT,
    DEFAULT_HTTP_PORT,
    DOMAIN,
)
from .coordinator import ShellyBridgeCoordinator
from .http_server import ShellyHttpServer
from .mdns_service import MdnsService

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["sensor"]


async def async_setup(hass: HomeAssistant, _config: dict) -> bool:
    hass.data.setdefault(DOMAIN, {})
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Shelly Pro 3EM Bridge from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    port = int(entry.data.get(CONF_HTTP_PORT, DEFAULT_HTTP_PORT))

    # 1 -- Coordinator
    coordinator = ShellyBridgeCoordinator(hass, entry)
    await coordinator.async_start()
    await coordinator.async_refresh()

    # 2 -- HTTP / WebSocket server
    http_server = ShellyHttpServer(coordinator, port)
    try:
        await http_server.async_start()
    except OSError as exc:
        _LOGGER.error("Cannot start HTTP server on port %d: %s", port, exc)
        await coordinator.async_shutdown()
        return False

    # 3 -- mDNS / Zeroconf
    mdns = MdnsService(hass, coordinator, port)
    await mdns.async_register()
    if mdns.host_ip:
        coordinator.host_ip = mdns.host_ip

    # 4a -- UDP broadcast listeners (local Zendure/Marstek discovery)
    from .udp_listener import UdpBroadcastListener
    udp_listener = UdpBroadcastListener(coordinator)
    await udp_listener.async_start()

    # 4b -- UDP RPC transport (Cloud-configured push to Hyper)
    from .udp_transport import UdpRpcTransport
    udp_transport = UdpRpcTransport(coordinator, 8006)
    await udp_transport.async_start()
    coordinator.udp_transport = udp_transport
    coordinator.rpc_udp_port = 8006

    # 5 -- Shelly Cloud client (optional)
    cloud_client = None
    cloud_enabled = entry.data.get(CONF_CLOUD_ENABLED, False)
    cloud_server = entry.data.get(CONF_CLOUD_SERVER, "")
    cloud_key = entry.data.get(CONF_CLOUD_KEY, "")

    if cloud_enabled and cloud_server and cloud_key:
        from .cloud_client import ShellyCloudClient
        cloud_client = ShellyCloudClient(
            coordinator=coordinator,
            cloud_server=cloud_server,
            cloud_key=cloud_key,
        )
        await cloud_client.async_start()

    # 6 -- Store references for teardown
    hass.data[DOMAIN][entry.entry_id] = {
        DATA_COORDINATOR: coordinator,
        DATA_HTTP_SERVER: http_server,
        DATA_MDNS: mdns,
        DATA_UDP_LISTENER: udp_listener,
        DATA_UDP_TRANSPORT: udp_transport,
        DATA_CLOUD_CLIENT: cloud_client,
    }

    # 7 -- Forward to sensor platform
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    _LOGGER.info("Bridge '%s' running on %s:%d", coordinator.device_id, mdns.host_ip, port)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a Shelly Pro 3EM Bridge config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if not unload_ok:
        return False

    entry_data = hass.data[DOMAIN].pop(entry.entry_id, {})

    # Cloud client
    cloud_client = entry_data.get(DATA_CLOUD_CLIENT)
    if cloud_client:
        await cloud_client.async_stop()

    # UDP transport
    udp_transport = entry_data.get(DATA_UDP_TRANSPORT)
    if udp_transport:
        await udp_transport.async_stop()

    # UDP listener
    udp_listener = entry_data.get(DATA_UDP_LISTENER)
    if udp_listener:
        await udp_listener.async_stop()

    # mDNS
    mdns = entry_data.get(DATA_MDNS)
    if mdns:
        await mdns.async_unregister()

    # HTTP server
    http_server = entry_data.get(DATA_HTTP_SERVER)
    if http_server:
        await http_server.async_stop()

    # Coordinator
    coordinator = entry_data.get(DATA_COORDINATOR)
    if coordinator:
        await coordinator.async_shutdown()

    return True
