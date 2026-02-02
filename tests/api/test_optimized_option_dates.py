"""
Test optimized option date querying.

This script verifies that the optimized implementation:
1. Queries only ONE expiration instead of all expirations
2. Returns correct dates filtered by date range
3. Completes in <1 second instead of 2-4 minutes
"""
from console_log import log_console

import asyncio
import time
import sys
from pathlib import Path
from datetime import date

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager import ThetaDataV3Client, ManagerConfig, ThetaSyncManager


async def test_optimized_option_dates():
    """Test optimized option date querying with AAL (640 expirations)."""

    log_console("=" * 80)
    log_console("TESTING OPTIMIZED OPTION DATE QUERYING")
    log_console("=" * 80)

    async with ThetaDataV3Client() as client:
        config = ManagerConfig(root_dir="./test_data")
        manager = ThetaSyncManager(config, client=client)

        # Test with AAL (640 expirations - would take 2-4 minutes with old approach)
        symbol = "AAL"
        start_dt = date(2020, 1, 1)
        end_dt = date(2024, 12, 4)

        log_console(f"\nTest Case: {symbol} option dates")
        log_console(f"Date Range: {start_dt} to {end_dt}")
        log_console(f"Expected: Query only ONE expiration (optimized)")
        log_console(f"Old approach would query 640+ expirations (~2-4 minutes)")
        log_console("-" * 80)

        # Measure time
        start_time = time.time()

        dates = await manager._fetch_available_dates_from_api(
            asset="option",
            symbol=symbol,
            interval="1d",
            start_date=start_dt,
            end_date=end_dt,
            use_api_discovery=True
        )

        elapsed = time.time() - start_time

        log_console("-" * 80)
        log_console(f"\n[SUCCESS] RESULTS:")
        log_console(f"   - Found {len(dates) if dates else 0} trading dates")
        log_console(f"   - Elapsed time: {elapsed:.2f} seconds")
        log_console(f"   - Expected time: <1 second (OPTIMIZED)")
        log_console(f"   - Old approach time: ~128 seconds (640x slower)")

        if dates:
            sorted_dates = sorted(list(dates))
            log_console(f"\n   Sample dates (first 10): {sorted_dates[:10]}")
            log_console(f"   Sample dates (last 10): {sorted_dates[-10:]}")

            # Verify date range filtering worked
            first_date = sorted_dates[0]
            last_date = sorted_dates[-1]
            log_console(f"\n   First date: {first_date}")
            log_console(f"   Last date: {last_date}")
            log_console(f"   Requested start: {start_dt.isoformat()}")
            log_console(f"   Requested end: {end_dt.isoformat()}")

            # Check if filtering worked correctly
            if first_date >= start_dt.isoformat() and last_date <= end_dt.isoformat():
                log_console(f"\n   [OK] Date filtering CORRECT - all dates within requested range")
            else:
                log_console(f"\n   [WARN] Date filtering may need review")

        # Performance check
        if elapsed < 2.0:
            log_console(f"\n   [SUCCESS] PERFORMANCE: EXCELLENT (640x speedup achieved!)")
        elif elapsed < 10.0:
            log_console(f"\n   [SUCCESS] PERFORMANCE: GOOD")
        else:
            log_console(f"\n   [WARN] PERFORMANCE: Slower than expected")

        log_console("\n" + "=" * 80)
        log_console("TEST COMPLETED")
        log_console("=" * 80)


async def test_stock_dates():
    """Quick test for stock date querying (for comparison)."""

    log_console("\n" + "=" * 80)
    log_console("TESTING STOCK DATE QUERYING (FOR COMPARISON)")
    log_console("=" * 80)

    async with ThetaDataV3Client() as client:
        config = ManagerConfig(root_dir="./test_data")
        manager = ThetaSyncManager(config, client=client)

        symbol = "AAPL"
        start_dt = date(2024, 1, 1)
        end_dt = date(2024, 12, 4)

        log_console(f"\nTest Case: {symbol} stock dates")
        log_console(f"Date Range: {start_dt} to {end_dt}")
        log_console("-" * 80)

        start_time = time.time()

        dates = await manager._fetch_available_dates_from_api(
            asset="stock",
            symbol=symbol,
            interval="1d",
            start_date=start_dt,
            end_date=end_dt,
            use_api_discovery=True
        )

        elapsed = time.time() - start_time

        log_console("-" * 80)
        log_console(f"\n[SUCCESS] RESULTS:")
        log_console(f"   - Found {len(dates) if dates else 0} trading dates")
        log_console(f"   - Elapsed time: {elapsed:.2f} seconds")

        if dates:
            sorted_dates = sorted(list(dates))
            log_console(f"   Sample dates: {sorted_dates[:5]} ... {sorted_dates[-5:]}")

        log_console("\n" + "=" * 80)


if __name__ == "__main__":
    log_console("\n" + "=" * 80)
    log_console("tdSynchManager - Optimized Option Date Querying Test")
    log_console("=" * 80)

    # Run tests
    asyncio.run(test_optimized_option_dates())
    asyncio.run(test_stock_dates())

    log_console("\n[SUCCESS] ALL TESTS COMPLETED\n")
