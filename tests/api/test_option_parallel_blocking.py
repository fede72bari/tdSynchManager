"""
Test parallel option API calls (AAL + XOM) to reproduce blocking issue
"""
from console_log import log_console

import asyncio
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager import ThetaDataV3Client, ManagerConfig, ThetaSyncManager, Task
from tdSynchManager.config import DiscoverPolicy

# Load environment variables
load_dotenv()

async def main():
    log_console("=" * 80)
    log_console("TEST: Parallel Option Processing (AAL + XOM)")
    log_console("=" * 80)

    influx_token = os.getenv('INFLUX_TOKEN')
    if not influx_token:
        raise ValueError("INFLUX_TOKEN environment variable is required. Please set it in your .env file.")

    cfg = ManagerConfig(
        root_dir=r"C:\\Users\\Federico\\Downloads",
        max_concurrency=80,
        influx_url=os.getenv('INFLUX_URL', 'http://127.0.0.1:8181'),
        influx_bucket=os.getenv('INFLUX_BUCKET', 'ThetaData'),
        influx_token=influx_token,
        influx_write_batch=5000,
    )

    tasks = [
        Task(
            asset="option",
            symbols=["AAL", "XOM"],
            intervals=["1d"],
            sink="influxdb",
            enrich_bar_greeks=True,
            first_date_override="20200102",
            end_date_override="20251125",
            ignore_existing=False,
            discover_policy=DiscoverPolicy(mode="mild_skip")
        ),
    ]

    log_console("\nThis should reproduce the blocking issue:")
    log_console("- AAL and XOM will be processed in parallel")
    log_console("- Both will call option_list_expirations at the same time")
    log_console("- System may block if concurrent calls aren't handled properly")
    log_console("-" * 80)

    async with ThetaDataV3Client(
        timeout_total=1800.0,
        timeout_sock_read=600.0
    ) as client:
        manager = ThetaSyncManager(cfg, client=client)
        await manager.run(tasks)

    log_console("\n" + "=" * 80)
    log_console("TEST COMPLETED")
    log_console("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
