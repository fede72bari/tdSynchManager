"""
Test InfluxDB MIN/MAX query performance
"""

import sys
from pathlib import Path
import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager.config import ManagerConfig
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.credentials import get_influx_credentials

def main():
    print("=" * 80)
    print("TEST: InfluxDB MIN/MAX Query Performance")
    print("=" * 80)

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
    print(f"\n{'='*80}")
    print("Test 1: XOM option 5m")
    print('='*80)

    t0 = time.time()
    first, last = manager._get_first_last_day_from_sink("option", "XOM", "5m", "influxdb")
    elapsed = time.time() - t0

    print(f"\n[RESULT] first={first}, last={last}")
    print(f"[RESULT] Total elapsed: {elapsed:.2f}s")

    # Test 2: AAL option 1d
    print(f"\n{'='*80}")
    print("Test 2: AAL option 1d")
    print('='*80)

    t0 = time.time()
    first, last = manager._get_first_last_day_from_sink("option", "AAL", "1d", "influxdb")
    elapsed = time.time() - t0

    print(f"\n[RESULT] first={first}, last={last}")
    print(f"[RESULT] Total elapsed: {elapsed:.2f}s")

    print("\n" + "=" * 80)
    print("TEST COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    main()
