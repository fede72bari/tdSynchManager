"""
Verify actual column names returned by ThetaData API for EOD option data
"""
from console_log import log_console

import asyncio
import pandas as pd
from src.tdSynchManager.ThetaDataV3Client import ThetaDataV3Client
from datetime import datetime

async def main():
    log_console("\n" + "="*80)
    log_console("COLUMN NAME VERIFICATION TEST")
    log_console("="*80)

    async with ThetaDataV3Client() as client:
        # Test EOD option data with greeks
        log_console("\nFetching EOD option data for TLRY (2025-11-19)...")

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

            log_console(f"\n[OK] Data fetched successfully!")
            log_console(f"   Rows: {len(df)}")
            log_console(f"\nActual columns returned by ThetaData API:")
            log_console("="*80)
            for i, col in enumerate(df.columns, 1):
                log_console(f"  {i:2d}. {col}")

            log_console(f"\nColumn name check:")
            log_console("="*80)
            if 'implied_vol' in df.columns:
                log_console("  [OK] 'implied_vol' - PRESENT (correct API V3 name)")
            else:
                log_console("  [ERROR] 'implied_vol' - MISSING")

            if 'implied_volatility' in df.columns:
                log_console("  [WARN] 'implied_volatility' - PRESENT (old name, unexpected)")
            else:
                log_console("  [OK] 'implied_volatility' - ABSENT (as expected)")

            log_console(f"\nSample data (first 3 rows):")
            log_console("="*80)
            log_console(df.head(3).to_string())

        except Exception as e:
            log_console(f"\n[ERROR] Error fetching data: {e}")
            import traceback
            traceback.print_exc()

    log_console("\n" + "="*80)
    log_console("TEST COMPLETE")
    log_console("="*80)

if __name__ == "__main__":
    asyncio.run(main())
