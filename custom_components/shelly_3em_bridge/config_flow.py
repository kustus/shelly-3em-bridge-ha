"""Config flow for the Shelly Pro 3EM Bridge.

Guides the user through setting up:
  1. The external MQTT broker and topic (the power source)
  2. The simulated device's MAC address (must be unique on the LAN)
  3. The HTTP server port
  4. (Optional) Shelly Cloud connection for Zendure Hyper 2000 Smart CT

The MAC address is critical: it must remain constant between restarts and
must be unique so the Shelly App does not confuse this virtual device with
any other Shelly or previously registered bridge instance.
"""

from __future__ import annotations

import logging
import re
import socket
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlow,
)
from homeassistant.core import callback
from homeassistant.helpers import selector

from .const import (
    CONF_CLOUD_ENABLED,
    CONF_CLOUD_KEY,
    CONF_CLOUD_SERVER,
    CONF_DEVICE_MAC,
    CONF_HTTP_PORT,
    CONF_MQTT_BROKER,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_TOPIC,
    CONF_MQTT_USERNAME,
    DEFAULT_CLOUD_KEY,
    DEFAULT_CLOUD_SERVER,
    DEFAULT_DEVICE_MAC,
    DEFAULT_HTTP_PORT,
    DEFAULT_MQTT_BROKER,
    DEFAULT_MQTT_PORT,
    DEFAULT_MQTT_TOPIC,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

_MAC_RE = re.compile(
    r"^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$"
)


class ShellyBridgeConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Shelly Pro 3EM Bridge."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Show the setup form and validate input."""
        errors: dict[str, str] = {}

        if user_input is not None:
            mac = user_input.get(CONF_DEVICE_MAC, "").upper().strip()
            # Normalize MAC to uppercase colon-separated
            mac_clean = mac.replace("-", ":").upper()
            user_input[CONF_DEVICE_MAC] = mac_clean

            # Validate MAC format
            if not _MAC_RE.match(mac_clean):
                errors[CONF_DEVICE_MAC] = "invalid_mac"

            # Validate port range (80 is the default and required for Shelly App scan)
            port = user_input.get(CONF_HTTP_PORT, DEFAULT_HTTP_PORT)
            if not 80 <= int(port) <= 65535:
                errors[CONF_HTTP_PORT] = "invalid_port"

            # Check that the HTTP port is not already in use
            if not errors and not await self.hass.async_add_executor_job(
                _is_port_available, int(port)
            ):
                errors[CONF_HTTP_PORT] = "port_in_use"

            if not errors:
                # Use MAC as unique ID to prevent duplicate entries
                await self.async_set_unique_id(mac_clean)
                self._abort_if_unique_id_configured()

                # Set cloud defaults so they exist in config entry
                user_input.setdefault(CONF_CLOUD_ENABLED, False)
                user_input.setdefault(CONF_CLOUD_SERVER, DEFAULT_CLOUD_SERVER)
                user_input.setdefault(CONF_CLOUD_KEY, DEFAULT_CLOUD_KEY)

                return self.async_create_entry(
                    title=f"Shelly Pro 3EM Bridge ({mac_clean[-8:]})",
                    data=user_input,
                )

        # Build the form schema with sensible defaults
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_MQTT_BROKER, default=DEFAULT_MQTT_BROKER
                ): str,
                vol.Required(
                    CONF_MQTT_PORT, default=DEFAULT_MQTT_PORT
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=1, max=65535, mode=selector.NumberSelectorMode.BOX
                    )
                ),
                vol.Required(
                    CONF_MQTT_TOPIC, default=DEFAULT_MQTT_TOPIC
                ): str,
                vol.Optional(CONF_MQTT_USERNAME, default=""): str,
                vol.Optional(CONF_MQTT_PASSWORD, default=""): selector.TextSelector(
                    selector.TextSelectorConfig(
                        type=selector.TextSelectorType.PASSWORD
                    )
                ),
                vol.Required(
                    CONF_DEVICE_MAC, default=DEFAULT_DEVICE_MAC
                ): str,
                vol.Required(
                    CONF_HTTP_PORT, default=DEFAULT_HTTP_PORT
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=80, max=65535, mode=selector.NumberSelectorMode.BOX
                    )
                ),
            }
        )

        return self.async_show_form(
            step_id="user",
            data_schema=schema,
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        return ShellyBridgeOptionsFlow()


class ShellyBridgeOptionsFlow(OptionsFlow):
    """Allow changing MQTT, port, and cloud settings without re-adding the entry."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            port = user_input.get(CONF_HTTP_PORT, DEFAULT_HTTP_PORT)
            if not 80 <= int(port) <= 65535:
                errors[CONF_HTTP_PORT] = "invalid_port"

            # If cloud enabled, key must not be empty
            cloud_enabled = user_input.get(CONF_CLOUD_ENABLED, False)
            cloud_key = user_input.get(CONF_CLOUD_KEY, "")
            if cloud_enabled and not cloud_key.strip():
                errors[CONF_CLOUD_KEY] = "cloud_key_required"

            if not errors:
                # Merge options into config entry data
                new_data = {**self.config_entry.data, **user_input}
                self.hass.config_entries.async_update_entry(
                    self.config_entry,
                    data=new_data,
                )
                return self.async_create_entry(title="", data={})

        current = self.config_entry.data
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_MQTT_BROKER,
                    default=current.get(CONF_MQTT_BROKER, DEFAULT_MQTT_BROKER),
                ): str,
                vol.Required(
                    CONF_MQTT_PORT,
                    default=current.get(CONF_MQTT_PORT, DEFAULT_MQTT_PORT),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=1, max=65535, mode=selector.NumberSelectorMode.BOX
                    )
                ),
                vol.Required(
                    CONF_MQTT_TOPIC,
                    default=current.get(CONF_MQTT_TOPIC, DEFAULT_MQTT_TOPIC),
                ): str,
                vol.Optional(
                    CONF_MQTT_USERNAME,
                    default=current.get(CONF_MQTT_USERNAME, ""),
                ): str,
                vol.Optional(
                    CONF_MQTT_PASSWORD,
                    default=current.get(CONF_MQTT_PASSWORD, ""),
                ): selector.TextSelector(
                    selector.TextSelectorConfig(
                        type=selector.TextSelectorType.PASSWORD
                    )
                ),
                vol.Required(
                    CONF_HTTP_PORT,
                    default=current.get(CONF_HTTP_PORT, DEFAULT_HTTP_PORT),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=80, max=65535, mode=selector.NumberSelectorMode.BOX
                    )
                ),
                # ── Shelly Cloud (for Zendure Hyper 2000) ────────────────
                vol.Optional(
                    CONF_CLOUD_ENABLED,
                    default=current.get(CONF_CLOUD_ENABLED, False),
                ): bool,
                vol.Optional(
                    CONF_CLOUD_SERVER,
                    default=current.get(CONF_CLOUD_SERVER, DEFAULT_CLOUD_SERVER),
                ): str,
                vol.Optional(
                    CONF_CLOUD_KEY,
                    default=current.get(CONF_CLOUD_KEY, DEFAULT_CLOUD_KEY),
                ): selector.TextSelector(
                    selector.TextSelectorConfig(
                        type=selector.TextSelectorType.PASSWORD
                    )
                ),
            }
        )

        return self.async_show_form(
            step_id="init",
            data_schema=schema,
            errors=errors,
        )


def _is_port_available(port: int) -> bool:
    """Check whether a TCP port is free on all interfaces (executor)."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("0.0.0.0", port))
        return True
    except OSError:
        return False
