"""Sensor platform for the Shelly Pro 3EM Bridge.

KEY FIX: Previous version used UnitOfPower.VOLT_AMPERE which does not exist
in HA and caused an AttributeError at import time:
  'type object UnitOfPower has no attribute VOLT_AMPERE'

Correct constants:
  - Watts       → UnitOfPower.WATT
  - Volt-Ampere → UnitOfApparentPower.VOLT_AMPERE
  - var         → UnitOfReactivePower.VOLT_AMPERE_REACTIVE
  - Energy      → UnitOfEnergy.WATT_HOUR / KILO_WATT_HOUR
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    UnitOfApparentPower,
    UnitOfElectricCurrent,
    UnitOfElectricPotential,
    UnitOfEnergy,
    UnitOfFrequency,
    UnitOfPower,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    CONF_DEVICE_MAC,
    DATA_COORDINATOR,
    DOMAIN,
    SHELLY_APP,
    SHELLY_DEVICE_NAME,
    SHELLY_FW_VERSION,
    SHELLY_MODEL,
)
from .coordinator import ShellyBridgeCoordinator


@dataclass(frozen=True, kw_only=True)
class ShellyBridgeSensorDescription(SensorEntityDescription):
    """Extended description with a data key for the coordinator dict."""

    data_key: str = ""
    scale: float = 1.0      # Optional scaling factor
    precision: int = 2      # Decimal places for rounding


# All sensors that the bridge exposes in Home Assistant.
# These mirror what a real Shelly Pro 3EM reports.
SENSOR_DESCRIPTIONS: tuple[ShellyBridgeSensorDescription, ...] = (
    ShellyBridgeSensorDescription(
        key="power_w",
        data_key="power_w",
        name="Active Power",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:lightning-bolt",
        precision=1,
    ),
    ShellyBridgeSensorDescription(
        key="voltage_v",
        data_key="voltage_v",
        name="Voltage",
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        device_class=SensorDeviceClass.VOLTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:sine-wave",
        precision=1,
    ),
    ShellyBridgeSensorDescription(
        key="current_a",
        data_key="current_a",
        name="Current",
        native_unit_of_measurement=UnitOfElectricCurrent.AMPERE,
        device_class=SensorDeviceClass.CURRENT,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:current-ac",
        precision=3,
    ),
    ShellyBridgeSensorDescription(
        key="frequency_hz",
        data_key="frequency_hz",
        name="Frequency",
        native_unit_of_measurement=UnitOfFrequency.HERTZ,
        device_class=SensorDeviceClass.FREQUENCY,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:chart-bell-curve",
        precision=2,
    ),
    ShellyBridgeSensorDescription(
        key="total_energy_wh",
        data_key="total_energy_wh",
        name="Total Energy",
        native_unit_of_measurement=UnitOfEnergy.WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:meter-electric",
        precision=2,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up sensor entities for this config entry."""
    coordinator: ShellyBridgeCoordinator = hass.data[DOMAIN][entry.entry_id][
        DATA_COORDINATOR
    ]
    async_add_entities(
        ShellyBridgeSensorEntity(coordinator, entry, desc)
        for desc in SENSOR_DESCRIPTIONS
    )


class ShellyBridgeSensorEntity(
    CoordinatorEntity[ShellyBridgeCoordinator], SensorEntity
):
    """A single sensor entity backed by the Shelly Bridge coordinator."""

    entity_description: ShellyBridgeSensorDescription
    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: ShellyBridgeCoordinator,
        entry: ConfigEntry,
        description: ShellyBridgeSensorDescription,
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        mac = entry.data[CONF_DEVICE_MAC].replace(":", "").lower()
        self._attr_unique_id = f"{DOMAIN}_{mac}_{description.key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, mac)},
            name=SHELLY_DEVICE_NAME,
            manufacturer="Allterco Robotics (Simulated)",
            model=SHELLY_MODEL,
            sw_version=SHELLY_FW_VERSION,
            configuration_url=(
                f"http://{entry.data.get('_host_ip', 'localhost')}:"
                f"{entry.data.get('http_port', 7580)}"
            ),
        )

    @property
    def native_value(self) -> float | None:
        """Return the sensor value from coordinator data."""
        data: dict[str, Any] | None = self.coordinator.data
        if data is None:
            return None
        raw = data.get(self.entity_description.data_key)
        if raw is None:
            return None
        return round(float(raw), self.entity_description.precision)

    @callback
    def _handle_coordinator_update(self) -> None:
        """React to coordinator data changes."""
        self.async_write_ha_state()
