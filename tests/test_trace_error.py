"""
Minimal test to trace the total_seconds error with full traceback
"""
from console_log import log_console
import asyncio
import traceback
from src.tdSynchManager.ThetaDataV3Client import ThetaDataV3Client
from src.tdSynchManager.manager import ThetaSyncManager
from src.tdSynchManager.config import ManagerConfig, Task, DiscoverPolicy
from src.tdSynchManager.credentials import get_influx_credentials

# Get InfluxDB credentials
influx = get_influx_credentials()
influx_token = influx['token']
influx_url = influx.get('url', 'http://127.0.0.1:8181')
influx_bucket = influx.get('bucket', 'ThetaData')

cfg = ManagerConfig(
    root_dir=r"tests/data",
    max_concurrency=1,  # Single thread for clearer tracing
    influx_url=influx_url,
    influx_bucket=influx_bucket,
    influx_token=influx_token,
    enable_data_validation=True,
    validation_strict_mode=True,
)

async def main():
    task = Task(
        asset="option",
        symbols=["TLRY"],
        intervals=["tick"],
        sink="influxdb",
        enrich_bar_greeks=True,
        enrich_tick_greeks=True,
        first_date_override="20250821",
        ignore_existing=False,
        discover_policy=DiscoverPolicy(mode="skip")
    )

    async with ThetaDataV3Client() as client:
        manager = ThetaSyncManager(cfg, client=client)
        try:
            await manager.run([task])
        except Exception as e:
            log_console("\n" + "="*80)
            log_console("FULL TRACEBACK:")
            log_console("="*80)
            traceback.print_exc()
            log_console("="*80)

if __name__ == "__main__":
    asyncio.run(main())
