"""
Quick test to debug where option_list_expirations is blocking
"""

import asyncio
import sys
from pathlib import Path
from datetime import date

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager import ThetaDataV3Client, ManagerConfig, ThetaSyncManager


async def test_option_blocking():
    """Test where option API calls block."""

    print("=" * 80)
    print("TEST: Option API Blocking Debug")
    print("=" * 80)

    async with ThetaDataV3Client() as client:
        config = ManagerConfig(root_dir="./test_data")
        manager = ThetaSyncManager(config, client=client)

        symbol = "AAL"
        start_dt = date(2020, 1, 2)
        end_dt = date(2025, 11, 25)

        print(f"\nSymbol: {symbol}")
        print(f"Date range: {start_dt} to {end_dt}")
        print(f"About to call _fetch_available_dates_from_api...")
        print("-" * 80)

        try:
            dates = await manager._fetch_available_dates_from_api(
                asset="option",
                symbol=symbol,
                interval="1d",
                start_date=start_dt,
                end_date=end_dt,
                use_api_discovery=True
            )

            print("-" * 80)
            print(f"\n[SUCCESS] Returned from _fetch_available_dates_from_api")
            print(f"Found {len(dates) if dates else 0} dates")

        except Exception as e:
            print(f"\n[ERROR] Exception: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_option_blocking())
