"""
Verify actual column names returned by ThetaData API for EOD option data
"""

import asyncio
import pandas as pd
from src.tdSynchManager.ThetaDataV3Client import ThetaDataV3Client
from datetime import datetime

async def main():
    print("\n" + "="*80)
    print("COLUMN NAME VERIFICATION TEST")
    print("="*80)

    async with ThetaDataV3Client() as client:
        # Test EOD option data with greeks
        print("\nFetching EOD option data for TLRY (2025-11-19)...")

        try:
            # Get option data with greeks for an intraday interval
            result = await client.option_history_all_greeks(
                symbol="TLRY",
                expiration=20251219,  # Dec 2025 expiration
                date=20251119,
                interval="5m",  # Use intraday interval (greeks endpoint doesn't support EOD)
                strike=2.0,
                right="C"
            )

            df = pd.DataFrame(result)

            print(f"\n[OK] Data fetched successfully!")
            print(f"   Rows: {len(df)}")
            print(f"\nActual columns returned by ThetaData API:")
            print("="*80)
            for i, col in enumerate(df.columns, 1):
                print(f"  {i:2d}. {col}")

            print(f"\nColumn name check:")
            print("="*80)
            if 'implied_vol' in df.columns:
                print("  [OK] 'implied_vol' - PRESENT (correct API V3 name)")
            else:
                print("  [ERROR] 'implied_vol' - MISSING")

            if 'implied_volatility' in df.columns:
                print("  [WARN] 'implied_volatility' - PRESENT (old name, unexpected)")
            else:
                print("  [OK] 'implied_volatility' - ABSENT (as expected)")

            print(f"\nSample data (first 3 rows):")
            print("="*80)
            print(df.head(3).to_string())

        except Exception as e:
            print(f"\n[ERROR] Error fetching data: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())
