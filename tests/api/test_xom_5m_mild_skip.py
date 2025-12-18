"""
Test XOM option 5m with mild_skip policy
This should now be FAST with metadata queries
"""

import asyncio
from tdSynchManager.manager import ThetaSyncManager, install_td_server_error_logger
from tdSynchManager.config import ManagerConfig, Task, DiscoverPolicy
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client
from tdSynchManager.credentials import get_influx_credentials

async def main():
    print("=" * 80)
    print("TEST: XOM option 5m with mild_skip (should be FAST now)")
    print("=" * 80)

    # Get InfluxDB credentials
    influx = get_influx_credentials()
    influx_token = influx['token']
    influx_url = influx.get('url', 'http://127.0.0.1:8181')
    influx_bucket = influx.get('bucket', 'ThetaData')

    cfg = ManagerConfig(
        root_dir=r"C:\\Users\\Federico\\Downloads",
        max_concurrency=80,
        influx_url=influx_url,
        influx_bucket=influx_bucket,
        influx_token=influx_token,
        influx_write_batch=5000,
    )

    tasks = [
        Task(
            asset="option",
            symbols=["XOM"],
            intervals=["5m"],
            sink="influxdb",
            enrich_bar_greeks=True,
            first_date_override="20200102",
            end_date_override="20251125",
            ignore_existing=False,
            discover_policy=DiscoverPolicy(mode="mild_skip")
        ),
    ]

    print(f"\nConfiguration:")
    print(f"  Symbol: XOM")
    print(f"  Interval: 5m (intraday - previously SLOW)")
    print(f"  Date range: 2020-01-02 to 2025-11-25")
    print(f"  Discover policy: mild_skip")
    print(f"  Expected: Fast metadata query (<1s instead of 4+ minutes)")
    print("-" * 80)

    async with ThetaDataV3Client(
        timeout_total=1800.0,
        timeout_sock_read=600.0
    ) as client:
        install_td_server_error_logger(client)
        manager = ThetaSyncManager(cfg, client=client)
        await manager.run(tasks)

    print("\n" + "=" * 80)
    print("TEST COMPLETED")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
