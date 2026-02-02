#!/usr/bin/env python3
"""Test script to debug InfluxDB listing in list_available_data()"""
from console_log import log_console

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

log_console("=" * 70)
log_console("TESTING INFLUXDB LISTING WITH DEBUG OUTPUT")
log_console("=" * 70)

# Create manager
cfg = ManagerConfig.from_env()
manager = ThetaSyncManager(cfg)

log_console("\nConfiguration:")
log_console(f"  influx_url: {cfg.influx_url}")
log_console(f"  influx_bucket: {cfg.influx_bucket}")
log_console(f"  influx_token: {'***' + cfg.influx_token[-4:] if cfg.influx_token else None}")

log_console("\n" + "=" * 70)
log_console("CALLING list_available_data()")
log_console("=" * 70)

available = manager.list_available_data()

log_console("\n" + "=" * 70)
log_console(f"RESULTS: Found {len(available)} data series")
log_console("=" * 70)
log_console(available)
