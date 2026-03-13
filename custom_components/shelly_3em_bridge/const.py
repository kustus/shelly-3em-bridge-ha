"""Constants for the Shelly Pro 3EM Bridge integration.

All protocol-relevant constants are centralized here to ensure
consistency across HTTP responses, WebSocket frames, and mDNS
service registration – the root cause of duplicate device entries
is inconsistent IDs across communication channels.
"""

# Integration domain
DOMAIN = "shelly_3em_bridge"

# ─── Shelly Device Identity ───────────────────────────────────────────────────
# These values MUST match across HTTP /shelly, /rpc responses, WebSocket
# frames, and mDNS TXT records. Any mismatch causes the Shelly App to
# register the same device multiple times.
SHELLY_MODEL = "SPEM-003CEBEU63"  # Real Pro 3EM-3CT63 hardware model string
SHELLY_APP = "Pro3EM"             # Application identifier (shows in Shelly App)
SHELLY_GEN = 2                    # Generation 2 = RPC-based
SHELLY_FW_VERSION = "1.7.1"       # Match real device firmware
SHELLY_FW_ID = "20250924-062749/1.7.1-gd336f31"
SHELLY_DEVICE_NAME = None         # Real device has name=null
SHELLY_PROFILE = "triphase"       # triphase = em:0/emdata:0 (vs monophase)
# Device ID prefix – the 63 model uses "shellypro3em63-" not "shellypro3em-"
SHELLY_ID_PREFIX = "shellypro3em63"

# Standard Shelly UDP RPC ports that Zendure/Marstek devices query via broadcast
SHELLY_BROADCAST_PORTS = [1010, 2220]

# Compact JSON encoder for UDP (no spaces, like real Shelly)
COMPACT_JSON = {"separators": (",", ":")}

# mDNS service type used by all Shelly Gen2 devices
MDNS_SERVICE_TYPE = "_shelly._tcp.local."

# ─── Simulated Physics ────────────────────────────────────────────────────────
SIMULATED_VOLTAGE_V = 230.0      # Nominal grid voltage (V)
SIMULATED_FREQUENCY_HZ = 50.0   # Nominal grid frequency (Hz)

# ─── Config Entry Keys ───────────────────────────────────────────────────────
CONF_MQTT_BROKER = "mqtt_broker"
CONF_MQTT_PORT = "mqtt_port"
CONF_MQTT_TOPIC = "mqtt_topic"
CONF_MQTT_USERNAME = "mqtt_username"
CONF_MQTT_PASSWORD = "mqtt_password"
CONF_DEVICE_MAC = "device_mac"
CONF_HTTP_PORT = "http_port"
CONF_CLOUD_SERVER = "cloud_server"
CONF_CLOUD_KEY = "cloud_key"
CONF_CLOUD_ENABLED = "cloud_enabled"

# ─── Defaults ────────────────────────────────────────────────────────────────
DEFAULT_MQTT_BROKER = "192.168.178.16"
DEFAULT_MQTT_PORT = 1883
DEFAULT_MQTT_TOPIC = "power/current"
DEFAULT_HTTP_PORT = 80
# NOTE: The Shelly App ALWAYS connects on port 80 during a network scan,
# regardless of what port the mDNS record advertises.  The HTTP server
# MUST therefore bind to port 80.  On HAOS this port is free by default
# (HA Core uses 8123).  If the Nginx SSL add-on is active it occupies
# port 80 → in that case use manual device-add in the Shelly App instead.

# Default MAC – the user MUST change this to a value unique on their LAN.
# Format: uppercase hex, colon-separated (AA:BB:CC:DD:EE:FF)
# This prefix uses the Allterco Robotics OUI (Shelly manufacturer).
DEFAULT_DEVICE_MAC = "AC:15:18:6C:51:D0"  # MAC of real Shelly Pro 3EM

# Cloud defaults – from real Shelly Pro 3EM
DEFAULT_CLOUD_SERVER = "shelly-171-eu.shelly.cloud:6022/jrpc"
DEFAULT_CLOUD_KEY = ""  # JWT must be extracted from real device

# ─── hass.data Keys ──────────────────────────────────────────────────────────
DATA_COORDINATOR = "coordinator"
DATA_HTTP_SERVER = "http_server"
DATA_MDNS = "mdns_info"
DATA_CLOUD_CLIENT = "cloud_client"
DATA_UDP_LISTENER = "udp_listener"
DATA_UDP_TRANSPORT = "udp_transport"
