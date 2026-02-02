"""
Test InfluxDB MIN/MAX query performance
"""
from console_log import log_console

import sys
from pathlib import Path
import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager.config import ManagerConfig
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.credentials import get_influx_credentials

def main():
    log_console("=" * 80)
    log_console("TEST: InfluxDB MIN/MAX Query Performance")
    log_console("=" * 80)

    # Get InfluxDB credentials
    influx = get_influx_credentials()
    influx_token = influx['token']
    influx_url = influx.get('url', 'http://127.0.0.1:8181')
    influx_bucket = influx.get('bucket', 'ThetaData')

    cfg = ManagerConfig(
        root_dir=r"tests/data",
        influx_url=influx_url,
        influx_bucket=influx_bucket,
        influx_token=influx_token,
    )

    manager = ThetaSyncManager(cfg, client=None)

    # Test 1: XOM option 5m (the problematic measurement)
    log_console(f"\n{'='*80}")
    log_console("Test 1: XOM option 5m")
    log_console('='*80)

    t0 = time.time()
    first, last = manager._get_first_last_day_from_sink("option", "XOM", "5m", "influxdb")
    elapsed = time.time() - t0

    log_console(f"\n[RESULT] first={first}, last={last}")
    log_console(f"[RESULT] Total elapsed: {elapsed:.2f}s")

    # Test 2: AAL option 1d
    log_console(f"\n{'='*80}")
    log_console("Test 2: AAL option 1d")
    log_console('='*80)

    t0 = time.time()
    first, last = manager._get_first_last_day_from_sink("option", "AAL", "1d", "influxdb")
    elapsed = time.time() - t0

    log_console(f"\n[RESULT] first={first}, last={last}")
    log_console(f"[RESULT] Total elapsed: {elapsed:.2f}s")

    log_console("\n" + "=" * 80)
    log_console("TEST COMPLETED")
    log_console("=" * 80)


if __name__ == "__main__":
    main()
