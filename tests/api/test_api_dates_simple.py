"""
Simple test of API date discovery with detailed logging
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager.manager import ThetaSyncManager, install_td_server_error_logger
from tdSynchManager.config import ManagerConfig, Task, DiscoverPolicy
from tdSynchManager.client import ThetaDataV3Client


async def main():
    print("=" * 80)
    print("TEST: API Date Discovery - Single Symbol (AAL)")
    print("=" * 80)

    influx_token = 'apiv3_reUhe6AEm4FjG4PHtLEW5wbt8MVUtiRtHPgm3Qw487pJFpVj6DlPTRxR1tvcW8bkY1IPM_PQEzHn5b1DVwZc2w'

    cfg = ManagerConfig(
        root_dir=r"C:\\Users\\Federico\\Downloads",
        max_concurrency=80,
        influx_url="http://127.0.0.1:8181",
        influx_bucket="ThetaData",
        influx_token=influx_token,
        influx_write_batch=5000,
    )

    tasks = [
        Task(
            asset="option",
            symbols=["AAL"],  # SINGLE SYMBOL
            intervals=["1d"],
            sink="influxdb",
            enrich_bar_greeks=True,
            first_date_override="20200102",
            end_date_override="20251125",
            ignore_existing=False,
            discover_policy=DiscoverPolicy(mode="mild_skip"),
            use_api_date_discovery=True  # Explicitly enable
        ),
    ]

    print(f"\nConfiguration:")
    print(f"  Symbol: AAL (SINGLE)")
    print(f"  Interval: 1d")
    print(f"  Date range: 2020-01-02 to 2025-11-25")
    print(f"  API date discovery: ENABLED")
    print("-" * 80)

    async with ThetaDataV3Client(
        timeout_total=1800.0,
        timeout_sock_read=600.0
    ) as client:
        install_td_server_error_logger(client)
        manager = ThetaSyncManager(cfg, client=client)

        print("\n[MAIN] Starting manager.run()...")
        await manager.run(tasks)
        print("[MAIN] manager.run() completed")

    print("\n" + "=" * 80)
    print("TEST COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
