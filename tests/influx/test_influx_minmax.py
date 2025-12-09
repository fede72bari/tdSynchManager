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

def main():
    print("=" * 80)
    print("TEST: InfluxDB MIN/MAX Query Performance")
    print("=" * 80)

    influx_token = 'apiv3_reUhe6AEm4FjG4PHtLEW5wbt8MVUtiRtHPgm3Qw487pJFpVj6DlPTRxR1tvcW8bkY1IPM_PQEzHn5b1DVwZc2w'

    cfg = ManagerConfig(
        root_dir=r"C:\Users\Federico\Downloads",
        influx_url="http://127.0.0.1:8181",
        influx_bucket="ThetaData",
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
