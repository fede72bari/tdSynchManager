"""
Test complete user tasks (AAL + XOM, options 1d + 5m, mild_skip)
"""
from console_log import log_console

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager.manager import ThetaSyncManager, install_td_server_error_logger
from tdSynchManager.config import ManagerConfig, Task, DiscoverPolicy
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client
from tdSynchManager.credentials import get_influx_credentials


async def main():
    log_console("=" * 80)
    log_console("TEST: User Tasks (AAL + XOM, options 1d + 5m, mild_skip)")
    log_console("=" * 80)

    # Get InfluxDB credentials
    influx = get_influx_credentials()
    influx_token = influx['token']
    influx_url = influx.get('url', 'http://127.0.0.1:8181')
    influx_bucket = influx.get('bucket', 'ThetaData')

    symbols = ["AAL", "XOM"]

    cfg = ManagerConfig(
        root_dir=r"C:\\Users\\Federico\\Downloads",
        max_concurrency=80,
        max_file_mb=16,
        overlap_seconds=60,
        influx_url=influx_url,
        influx_bucket=influx_bucket,
        influx_token=influx_token,
        influx_org=None,
        influx_precision="nanosecond",
        influx_measure_prefix="",
        influx_write_batch=5000,
        enable_data_validation=True,
        validation_strict_mode=False,
    )

    tasks = [
        Task(
            asset="option",
            symbols=symbols,
            intervals=["1d"],
            sink="influxdb",
            enrich_bar_greeks=True,
            enrich_tick_greeks=True,
            first_date_override="20200102",
            end_date_override="20251125",
            ignore_existing=False,
            discover_policy=DiscoverPolicy(mode="mild_skip")
        ),

        Task(
            asset="option",
            symbols=symbols,
            intervals=["5m"],
            sink="influxdb",
            enrich_bar_greeks=True,
            enrich_tick_greeks=True,
            first_date_override="20200102",
            end_date_override="20251125",
            ignore_existing=False,
            discover_policy=DiscoverPolicy(mode="mild_skip")
        ),
    ]

    log_console(f"\nConfiguration:")
    log_console(f"  Symbols: {symbols}")
    log_console(f"  Intervals: 1d, 5m")
    log_console(f"  Date range: 2020-01-02 to 2025-11-25")
    log_console(f"  Discover policy: mild_skip")
    log_console(f"  Max concurrency: {cfg.max_concurrency}")
    log_console("-" * 80)

    async with ThetaDataV3Client(
        timeout_total=1800.0,
        timeout_sock_read=600.0
    ) as client:
        install_td_server_error_logger(client)
        manager = ThetaSyncManager(cfg, client=client)
        await manager.run(tasks)

    log_console("\n" + "=" * 80)
    log_console("TEST COMPLETED")
    log_console("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
