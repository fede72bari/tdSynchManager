#!/usr/bin/env python3
"""Test script to debug InfluxDB listing in list_available_data()"""

import sys
import importlib

# Force reload of the module to ensure we have the latest version
if 'tdSynchManager' in sys.modules:
    del sys.modules['tdSynchManager']
if 'tdSynchManager.manager' in sys.modules:
    del sys.modules['tdSynchManager.manager']
if 'tdSynchManager.config' in sys.modules:
    del sys.modules['tdSynchManager.config']

# Now import
from src.tdSynchManager.config import ManagerConfig
from src.tdSynchManager.manager import ThetaSyncManager

print("=" * 70)
print("TESTING INFLUXDB LISTING WITH DEBUG OUTPUT")
print("=" * 70)

# Create manager
cfg = ManagerConfig.from_env()
manager = ThetaSyncManager(cfg)

print("\nConfiguration:")
print(f"  influx_url: {cfg.influx_url}")
print(f"  influx_bucket: {cfg.influx_bucket}")
print(f"  influx_token: {'***' + cfg.influx_token[-4:] if cfg.influx_token else None}")

print("\n" + "=" * 70)
print("CALLING list_available_data()")
print("=" * 70)

available = manager.list_available_data()

print("\n" + "=" * 70)
print(f"RESULTS: Found {len(available)} data series")
print("=" * 70)
print(available)
