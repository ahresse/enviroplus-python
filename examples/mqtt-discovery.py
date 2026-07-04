#!/usr/bin/env python3
"""
Home Assistant MQTT Discovery for Enviro+

Publishes all sensors via MQTT with HA auto-discovery.
Requires mosquitto broker: sudo apt-get install mosquitto mosquitto-clients

Usage:
  python3 mqtt-discovery.py --broker 192.168.1.100
  python3 mqtt-discovery.py --broker 192.168.1.100 --username user --password pass
"""

import argparse
import json
import math
import signal
import sys
import time
from subprocess import PIPE, Popen

import numpy as np

from bme280 import BME280

from enviroplus import gas

try:
    from smbus2 import SMBus
except ImportError:
    from smbus import SMBus

import paho.mqtt.client as mqtt

try:
    from ltr559 import LTR559
    ltr559 = LTR559()
except ImportError:
    import ltr559

# Optional sensors
HAS_GAS = False
HAS_PMS = False
HAS_NOISE = False

try:
    from pms5003 import PMS5003, ReadTimeoutError, SerialTimeoutError
except ImportError:
    PMS5003 = None

try:
    from enviroplus.noise import Noise
    HAS_NOISE = True
except ImportError:
    Noise = None


DISCOVERY_PREFIX = "homeassistant"
STATE_TOPIC_PREFIX = "enviroplus"
DEVICE_MANUFACTURER = "Pimoroni"
DEVICE_MODEL = "Enviro+"
SW_VERSION = "1.0"

running = True


class GasCalibrator:
    def __init__(self, warmup=120):
        self.warmup = warmup
        self.start_time = time.time()
        self.calibrated = False
        self.samples = []
        self.r0 = {}

    def feed(self, oxidising, reducing, nh3):
        if self.calibrated:
            return
        self.samples.append((oxidising, reducing, nh3))
        elapsed = time.time() - self.start_time
        if elapsed >= self.warmup and len(self.samples) >= 5:
            ox_vals = [s[0] for s in self.samples]
            red_vals = [s[1] for s in self.samples]
            nh3_vals = [s[2] for s in self.samples]
            self.r0["oxidising"] = min(ox_vals)
            self.r0["reducing"] = max(red_vals)
            self.r0["nh3"] = max(nh3_vals)
            self.calibrated = True

    def to_ppm(self, gas, resistance):
        if not self.calibrated or gas not in self.r0 or self.r0[gas] == 0:
            return None
        rs_r0 = resistance / self.r0[gas]
        if rs_r0 <= 0:
            return None
        log_ratio = math.log10(rs_r0)
        if gas == "oxidising":
            return round(10 ** ((log_ratio - 0.544) / 0.223), 1)
        elif gas == "reducing":
            return round(10 ** ((log_ratio + 0.125) / -0.382), 1)
        elif gas == "nh3":
            return round(10 ** ((log_ratio + 0.200) / -0.400), 1)
        return None


def signal_handler(sig, frame):
    global running
    running = False


def get_cpu_temperature():
    process = Popen(["vcgencmd", "measure_temp"], stdout=PIPE, universal_newlines=True)
    output, _error = process.communicate()
    return float(output[output.index("=") + 1 : output.rindex("'")])


def get_serial_number():
    with open("/proc/cpuinfo", "r") as f:
        for line in f:
            if line[0:6] == "Serial":
                return line.split(":")[1].strip()
    return "unknown"


def build_sensor_registry(serial):
    device_id = f"enviroplus_{serial}"

    device_registry = {
        "identifiers": [device_id],
        "name": "Enviro+",
        "manufacturer": DEVICE_MANUFACTURER,
        "model": DEVICE_MODEL,
        "sw_version": SW_VERSION,
    }

    sensors = [
        {
            "name": "Temperature",
            "unique_id": f"{device_id}_temperature",
            "state_topic": f"{STATE_TOPIC_PREFIX}/{serial}/temperature/state",
            "unit_of_measurement": "°C",
            "device_class": "temperature",
            "state_class": "measurement",
        },
        {
            "name": "Pressure",
            "unique_id": f"{device_id}_pressure",
            "state_topic": f"{STATE_TOPIC_PREFIX}/{serial}/pressure/state",
            "unit_of_measurement": "hPa",
            "device_class": "pressure",
            "state_class": "measurement",
        },
        {
            "name": "Humidity",
            "unique_id": f"{device_id}_humidity",
            "state_topic": f"{STATE_TOPIC_PREFIX}/{serial}/humidity/state",
            "unit_of_measurement": "%",
            "device_class": "humidity",
            "state_class": "measurement",
        },
        {
            "name": "Light",
            "unique_id": f"{device_id}_light",
            "state_topic": f"{STATE_TOPIC_PREFIX}/{serial}/light/state",
            "unit_of_measurement": "lx",
            "device_class": "illuminance",
            "state_class": "measurement",
        },
        {
            "name": "Proximity",
            "unique_id": f"{device_id}_proximity",
            "state_topic": f"{STATE_TOPIC_PREFIX}/{serial}/proximity/state",
        },
    ]

    if HAS_GAS:
        sensors.extend([
            {
                "name": "Nitrogen Dioxide (NO2)",
                "unique_id": f"{device_id}_no2",
                "state_topic": f"{STATE_TOPIC_PREFIX}/{serial}/no2/state",
                "unit_of_measurement": "ppm",
                "state_class": "measurement",
            },
            {
                "name": "Carbon Monoxide (CO)",
                "unique_id": f"{device_id}_co",
                "state_topic": f"{STATE_TOPIC_PREFIX}/{serial}/co/state",
                "unit_of_measurement": "ppm",
                "state_class": "measurement",
            },
            {
                "name": "Ammonia (NH3)",
                "unique_id": f"{device_id}_nh3",
                "state_topic": f"{STATE_TOPIC_PREFIX}/{serial}/nh3/state",
                "unit_of_measurement": "ppm",
                "state_class": "measurement",
            },
        ])

    sensors.extend([
        {
            "name": "CPU Temperature",
            "unique_id": f"{device_id}_cpu_temperature",
            "state_topic": f"{STATE_TOPIC_PREFIX}/{serial}/cpu_temperature/state",
            "unit_of_measurement": "°C",
            "device_class": "temperature",
            "state_class": "measurement",
            "entity_category": "diagnostic",
        },
    ])

    if HAS_PMS:
        sensors.extend([
            {
                "name": "PM1.0",
                "unique_id": f"{device_id}_pm1",
                "state_topic": f"{STATE_TOPIC_PREFIX}/{serial}/pm1/state",
                "unit_of_measurement": "µg/m³",
                "device_class": "pm1",
                "state_class": "measurement",
            },
            {
                "name": "PM2.5",
                "unique_id": f"{device_id}_pm25",
                "state_topic": f"{STATE_TOPIC_PREFIX}/{serial}/pm25/state",
                "unit_of_measurement": "µg/m³",
                "device_class": "pm25",
                "state_class": "measurement",
            },
            {
                "name": "PM10",
                "unique_id": f"{device_id}_pm10",
                "state_topic": f"{STATE_TOPIC_PREFIX}/{serial}/pm10/state",
                "unit_of_measurement": "µg/m³",
                "device_class": "pm10",
                "state_class": "measurement",
            },
        ])

    if HAS_NOISE:
        sensors.extend([
            {
                "name": "Noise Level",
                "unique_id": f"{device_id}_noise",
                "state_topic": f"{STATE_TOPIC_PREFIX}/{serial}/noise/state",
                "unit_of_measurement": "dBFS",
                "state_class": "measurement",
            },
        ])

    for s in sensors:
        s["device"] = device_registry

    return sensors


def publish_discovery(client, sensors, prefix, retain):
    for s in sensors:
        topic = f"{prefix}/sensor/{s['unique_id']}/config"
        payload = {k: v for k, v in s.items()}
        client.publish(topic, json.dumps(payload), retain=retain)
        print(f"  Discovery: {topic}")


def read_sensors(bme280, pms5003_inst, noise, calibrator):
    global HAS_GAS

    values = {}

    cpu_temp = get_cpu_temperature()
    raw_temp = bme280.get_temperature()
    comp_factor = 2.25
    comp_temp = raw_temp - ((cpu_temp - raw_temp) / comp_factor)

    values["temperature"] = round(comp_temp, 1)
    values["pressure"] = round(bme280.get_pressure(), 1)
    values["humidity"] = round(bme280.get_humidity(), 1)
    values["cpu_temperature"] = round(cpu_temp, 1)

    values["light"] = round(ltr559.get_lux(), 1)
    values["proximity"] = ltr559.get_proximity()

    if HAS_GAS:
        gas_reading = gas.read_all()
        if calibrator and calibrator.calibrated:
            no2 = calibrator.to_ppm("oxidising", gas_reading.oxidising)
            co = calibrator.to_ppm("reducing", gas_reading.reducing)
            nh3 = calibrator.to_ppm("nh3", gas_reading.nh3)
            if no2 is not None:
                values["no2"] = no2
            if co is not None:
                values["co"] = co
            if nh3 is not None:
                values["nh3"] = nh3
        else:
            if calibrator:
                calibrator.feed(gas_reading.oxidising, gas_reading.reducing, gas_reading.nh3)

    if HAS_PMS and pms5003_inst:
        try:
            pm_data = pms5003_inst.read()
            values["pm1"] = pm_data.pm_ug_per_m3(1)
            values["pm25"] = pm_data.pm_ug_per_m3(2.5)
            values["pm10"] = pm_data.pm_ug_per_m3(10)
        except (ReadTimeoutError, SerialTimeoutError):
            pms5003_inst.reset()
            pm_data = pms5003_inst.read()
            values["pm1"] = pm_data.pm_ug_per_m3(1)
            values["pm25"] = pm_data.pm_ug_per_m3(2.5)
            values["pm10"] = pm_data.pm_ug_per_m3(10)

    if HAS_NOISE and noise:
        try:
            recording = noise._record()
            rms = np.sqrt(np.mean(recording ** 2))
            if rms > 0:
                values["noise"] = round(20 * math.log10(rms), 1)
        except Exception:
            pass

    return values


def main():
    global HAS_GAS, HAS_NOISE, HAS_PMS, pms5003

    parser = argparse.ArgumentParser(
        description="Enviro+ MQTT with Home Assistant Discovery"
    )
    parser.add_argument("--broker", default="localhost", help="MQTT broker address")
    parser.add_argument("--port", default=1883, type=int, help="MQTT broker port")
    parser.add_argument("--username", default=None, help="MQTT username")
    parser.add_argument("--password", default=None, help="MQTT password")
    parser.add_argument(
        "--interval", default=30, type=int, help="Publish interval in seconds"
    )
    parser.add_argument(
        "--discovery-prefix",
        default=DISCOVERY_PREFIX,
        help="Home Assistant discovery prefix",
    )
    parser.add_argument(
        "--no-retain",
        action="store_true",
        help="Disable retain flag on discovery topics",
    )
    parser.add_argument("--tls", action="store_true", help="Enable TLS")
    parser.add_argument(
        "--calibration-time",
        default=120,
        type=int,
        help="Gas sensor warm-up/calibration time in seconds",
    )
    args = parser.parse_args()

    serial = get_serial_number()
    device_id = f"raspi-{serial}"

    print(f"Enviro+ MQTT Discovery")
    print(f"  Device:       {device_id}")
    print(f"  Broker:       {args.broker}:{args.port}")
    print(f"  Interval:     {args.interval}s")
    print(f"  Discovery:    {args.discovery_prefix}")

    # Init I2C
    bus = SMBus(1)
    bme280 = BME280(i2c_dev=bus)

    # Detect gas sensor
    if gas.available():
        HAS_GAS = True
        print(f"  Gas sensor:   connected")
    else:
        print(f"  Gas sensor:   not connected")

    # Detect PMS5003
    if PMS5003 is not None:
        try:
            pms5003 = PMS5003()
            _ = pms5003.read()
            HAS_PMS = True
            print(f"  PMS5003:      connected")
        except SerialTimeoutError:
            print(f"  PMS5003:      not connected")

    # Init noise
    noise = None
    if HAS_NOISE:
        try:
            noise = Noise(sample_rate=16000, duration=0.5)
            print(f"  Noise:        enabled")
        except Exception:
            HAS_NOISE = False
            print(f"  Noise:        not available")

    # Gas sensor calibration
    calibrator = GasCalibrator(warmup=args.calibration_time)
    print(f"  Calibration:  {args.calibration_time}s warm-up for gas sensor")

    # Build sensor registry
    sensors = build_sensor_registry(serial)
    print(f"  Sensors:      {len(sensors)} entities")

    # MQTT client
    client = mqtt.Client(client_id=device_id)
    if args.username and args.password:
        client.username_pw_set(args.username, args.password)
    if args.tls:
        client.tls_set()

    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            print("  MQTT:         connected")
            retain = not args.no_retain
            publish_discovery(c, sensors, args.discovery_prefix, retain)
        else:
            print(f"  MQTT:         connection failed (rc={rc})")

    client.on_connect = on_connect
    client.connect(args.broker, port=args.port)
    client.loop_start()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("\nPublishing sensor data... (press Ctrl+C to stop)\n")
    calib_notified = False

    while running:
        try:
            values = read_sensors(bme280, pms5003 if HAS_PMS else None, noise, calibrator)

            if calibrator.calibrated and not calib_notified:
                calib_notified = True
                print("  Calibration:  complete, gas values now in ppm")

            for key, value in values.items():
                topic = f"{STATE_TOPIC_PREFIX}/{serial}/{key}/state"
                client.publish(topic, str(value))

            combined_topic = f"{STATE_TOPIC_PREFIX}/{serial}/values"
            client.publish(combined_topic, json.dumps(values))

            if int(time.time()) % (args.interval * 2) < args.interval:
                print(f"  Published {len(values)} values at {time.strftime('%H:%M:%S')}")

        except Exception as e:
            print(f"  Error: {e}", file=sys.stderr)

        time.sleep(args.interval)

    print("\nShutting down...")
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()
