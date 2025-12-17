"""
Minimal test to trace the total_seconds error with full traceback
"""
import asyncio
import traceback
from src.tdSynchManager.ThetaDataV3Client import ThetaDataV3Client
from src.tdSynchManager.manager import ThetaSyncManager
from src.tdSynchManager.config import ManagerConfig, Task, DiscoverPolicy

cfg = ManagerConfig(
    root_dir=r"C:\Users\Federico\Downloads",
    max_concurrency=1,  # Single thread for clearer tracing
    influx_url="http://127.0.0.1:8181",
    influx_bucket="ThetaData",
    influx_token='apiv3_reUhe6AEm4FjG4PHtLEW5wbt8MVUtiRtHPgm3Qw487pJFpVj6DlPTRxR1tvcW8bkY1IPM_PQEzHn5b1DVwZc2w',
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
            print("\n" + "="*80)
            print("FULL TRACEBACK:")
            print("="*80)
            traceback.print_exc()
            print("="*80)

if __name__ == "__main__":
    asyncio.run(main())
