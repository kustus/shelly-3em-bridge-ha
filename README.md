# Shelly Pro 3EM Bridge – Home Assistant Custom Integration

Eine Home Assistant Custom Integration, die ein **Shelly Pro 3EM Gen2** Gerät im
lokalen Netzwerk simuliert. Sie liest einen Leistungswert (Watt) von einem externen
MQTT-Broker und stellt ihn über alle Shelly-Protokolle bereit – so dass ein
**Zendure Hyper 2000** (oder Marstek) die Daten als echtes Shelly-Gerät erkennt.

## Funktionsweise

```
┌──────────────┐     MQTT      ┌───────────────────────────────────┐
│  IR-Leser /  │ ──────────▶  │  Home Assistant                   │
│  Stromzähler │   (Watt)     │                                   │
└──────────────┘              │  shelly_3em_bridge Integration:   │
                              │  ├─ coordinator.py  (MQTT + HA)   │
                              │  ├─ http_server.py  (HTTP/WS :80) │── Shelly App
                              │  ├─ mdns_service.py (mDNS)        │── Geräte-Discovery
                              │  ├─ udp_listener.py (:1010/2220)  │── Zendure (lokal)
                              │  ├─ udp_transport.py (:8006)      │── Zendure (Push)
                              │  ├─ cloud_client.py (WSS)         │── Shelly Cloud
                              │  └─ sensor.py       (HA Entities) │── HA Dashboard
                              └───────────────────────────────────┘
```

### Kommunikationswege zum Zendure Hyper 2000

| Weg | Beschreibung | Latenz |
|---|---|---|
| **UDP Broadcast (lokal)** | Hyper sendet Broadcasts an 1010/2220, Bridge antwortet | < 100ms |
| **UDP RPC Push** | Bridge pusht `NotifyStatus` an Hyper (Port 8006) | < 100ms |
| **Shelly Cloud (WSS)** | Bridge → Shelly Cloud → Zendure Cloud → Hyper | ~1-5s |

Der lokale WiFi-Weg wird bevorzugt. Die Cloud-Verbindung wird für die Ersteinrichtung
benötigt und als Fallback verwendet.

## Voraussetzungen

| Anforderung | Details |
|---|---|
| Home Assistant | 2024.x oder neuer (getestet auf HAOS 2026.2+) |
| MQTT-Broker | Erreichbar im LAN (z.B. Mosquitto) |
| MQTT-Topic | Liefert einen **reinen Float-Wert** in Watt, z.B. `178.5` |
| Port 80 | Muss frei sein (auf HAOS standardmäßig frei, HA nutzt 8123) |
| Echtes Shelly | Muss **ausgeschaltet** sein wenn Cloud aktiviert ist |

### Warum Port 80?

Die Shelly App und der Zendure Hyper ignorieren den Port aus dem mDNS-Record und
verbinden sich **immer** auf Port 80. Ein anderer Port führt zu "Gerät inaktiv".
Auf HAOS ist Port 80 frei (HA läuft auf 8123). Falls der Nginx-SSL-Add-on
aktiv ist, belegt dieser Port 80 – in dem Fall das Gerät manuell in der App hinzufügen.

## Installation

### Manuell (empfohlen)

```bash
# Auf dem HA-Rechner (SSH-Add-on oder Samba-Share)
cp -r custom_components/shelly_3em_bridge /config/custom_components/shelly_3em_bridge
```

Dann **Home Assistant komplett neustarten** (Einstellungen → System → Neustart).
"Neu laden" reicht nicht für Code-Änderungen.

### Via Samba

Den Ordner `custom_components/shelly_3em_bridge` auf den Samba-Share des HA-Rechners
kopieren, z.B.:

```
smb://192.168.178.7/config/custom_components/shelly_3em_bridge/
```

## Konfiguration

1. **Einstellungen → Geräte & Dienste → Integration hinzufügen**
2. Nach **Shelly Pro 3EM Bridge** suchen
3. Formular ausfüllen:

| Feld | Standard | Hinweis |
|---|---|---|
| MQTT Broker IP | `192.168.178.16` | IP des MQTT-Brokers |
| MQTT Port | `1883` | Standard-MQTT-Port |
| MQTT Topic | `power/current` | Topic mit Watt-Werten als Float |
| MQTT User/Passwort | — | Leer wenn keine Auth |
| **Device MAC** | `AC:15:18:6C:51:D0` | MAC des echten Shelly Pro 3EM |
| HTTP Port | `80` | **Muss 80 sein** für Shelly App/Hyper |

### Cloud-Einstellungen (optional)

Über **Einstellungen → Geräte & Dienste → Shelly Pro 3EM Bridge → Konfigurieren**
können nachträglich Cloud-Parameter gesetzt werden:

| Feld | Beschreibung |
|---|---|
| Cloud aktiviert | Verbindung zur Shelly Cloud herstellen |
| Cloud Server | `shelly-171-eu.shelly.cloud:6022/jrpc` |
| Cloud Key | JWT-Token vom echten Shelly Pro 3EM |

**Woher kommt der Cloud-Key?**
Der JWT-Token wird beim Einrichten des echten Shelly in der Cloud gespeichert.
Er kann über `Shelly.GetDeviceInfo` mit `ident=true` ausgelesen werden, solange
das echte Gerät erreichbar ist.

## Wichtig: MAC-Adresse

Die MAC-Adresse ist **kritisch**:

- **Muss die MAC des echten Shelly Pro 3EM sein**, wenn Cloud aktiviert ist
  (der JWT-Token ist an die MAC gebunden)
- **Muss konstant bleiben** – ändern erzeugt ein "neues" Gerät in der Shelly App
- **Muss konsistent sein** – die gleiche MAC wird in HTTP, WebSocket, mDNS und
  Cloud verwendet (Inkonsistenzen führen zu Duplikaten)

## HA Entities

Nach der Einrichtung erscheint ein Gerät **Shelly Pro 3EM** mit diesen Sensoren:

| Entity | Einheit | Quelle |
|---|---|---|
| Active Power | W | Direkt vom MQTT-Topic |
| Voltage | V | Simuliert (230 V) |
| Current | A | Berechnet (P / U) |
| Frequency | Hz | Simuliert (50 Hz) |
| Total Energy | Wh | Aufsummiert solange HA läuft |

## Architektur

| Datei | Funktion |
|---|---|
| `__init__.py` | Lifecycle: Setup und Teardown aller Komponenten |
| `coordinator.py` | DataUpdateCoordinator + paho-mqtt + Energy-Akkumulation |
| `http_server.py` | aiohttp Server: HTTP/WebSocket RPC-Protokoll (Port 80) |
| `mdns_service.py` | Zeroconf: `_shelly._tcp` + `_http._tcp` Registration |
| `udp_listener.py` | UDP Broadcast Listener (Ports 1010/2220) |
| `udp_transport.py` | UDP RPC Transport (Port 8006, Push an Hyper) |
| `cloud_client.py` | WebSocket-Client zur Shelly Cloud (WSS) |
| `sensor.py` | HA Sensor-Platform (5 Entities) |
| `config_flow.py` | UI Config Flow + Options Flow |
| `const.py` | Alle Konstanten (Single Source of Truth) |
| `manifest.json` | HA Integration Metadata |

### Datenfluss

1. MQTT-Nachricht mit Watt-Wert trifft ein (paho-mqtt Thread)
2. `coordinator.py` aktualisiert `ShellyMeterData` und HA-Sensoren
3. Alle Kanäle werden benachrichtigt:
   - WebSocket-Clients → `NotifyStatus`
   - UDP Transport → `NotifyStatus` an Hyper (rate-limited: max 1x/2s)
   - Cloud Client → periodischer Push (alle 30s)
4. Eingehende Anfragen (HTTP, WS, UDP) werden mit aktuellen Daten beantwortet

### Protokoll-Details

Die Integration implementiert das vollständige **Shelly Gen2 RPC-Protokoll**:

- `Shelly.GetDeviceInfo` – Geräte-Identität
- `Shelly.GetStatus` / `Shelly.GetConfig` – Vollstatus/Konfiguration
- `Shelly.GetComponents` – Komponentenliste
- `EM.GetStatus` – Aktuelle Leistungsdaten (3 Phasen)
- `EMData.GetStatus` / `EMData.GetData` – Energiezähler
- `Sys.SetConfig` – Konfiguration (rpc_udp für Hyper)
- `Shelly.Reboot` – Simulierter Neustart
- ~60 weitere Methoden (Stubs) für App-Kompatibilität

### Technische Details

- **Decimal-Enforce:** Zendure/Marstek erwarten Dezimalpunkte in JSON-Zahlen.
  `0` → `0.001`, `100.0` → `100.001`
- **Compact JSON:** UDP-Antworten ohne Leerzeichen (wie echte Shelly-Firmware)
- **Simulated Restart:** Cloud/Hyper-Reboot setzt Uptime-Counter zurück

## Fehlerbehebung

| Problem | Ursache | Lösung |
|---|---|---|
| "Gerät inaktiv" in App | Port ≠ 80 | HTTP Port auf 80 setzen |
| Gerät nicht gefunden | mDNS nicht erreichbar | Gleiches Subnetz? |
| Hyper "kein Smart CT" | Cloud nicht aktiv | Cloud aktivieren, Key prüfen |
| Doppelte Geräte | MAC geändert | Geräte löschen, MAC konstant halten |
| `address in use` | Port 80 belegt | Nginx-Add-on prüfen, HA neustarten |
| Sensoren "nicht verfügbar" | MQTT-Broker nicht erreichbar | Broker-IP/Topic prüfen |

## MQTT-Format

Das Topic muss einen **reinen Float-Wert** in Watt liefern:

```
178.5
```

Bei JSON-Payloads (z.B. `{"power": 178.5}`) einen Template-Sensor oder
Node-RED-Flow vorschalten, der den Wert extrahiert und als plain number
republished.

## Parallelbetrieb mit Standalone-Version

Es gibt eine zweite Version als **Standalone Python-Script** (Verzeichnis
`shelly_3em_standalone`). **Nur eine Version darf gleichzeitig laufen**, da beide
die gleichen Ports (80, 1010, 2220, 8006) und die gleiche Geräte-ID verwenden.

Wechsel zur Standalone:
1. HA-Integration unter Geräte & Dienste entfernen
2. `sudo systemctl enable --now shelly-bridge` auf dem Pi

Zurück zur HA-Integration:
1. `sudo systemctl stop shelly-bridge && sudo systemctl disable shelly-bridge`
2. Integration in HA hinzufügen, HA neustarten

## Versionshistorie

| Version | Änderungen |
|---|---|
| **3.0.0** | UDP Broadcast Listener, UDP RPC Transport, Cloud-Client mit Sys.SetConfig/Reboot, lokale WiFi-Kommunikation mit Zendure Hyper |
| **2.1.0** | Port-Verfügbarkeitsprüfung, Options Flow |
| **2.0.0** | Komplett-Rewrite: Shutdown-Fix, mDNS, MAC-Konsistenz |
| 1.x | Initiale Versionen |
