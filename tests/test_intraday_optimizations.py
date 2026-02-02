#!/usr/bin/env python3
"""
Test Intraday Pipeline Optimizations

Tests the two main optimizations implemented for real-time intraday downloads:
1. Skip API date fetch for single-day downloads
2. OI (Open Interest) caching to avoid redundant API calls

Expected behavior:
- First run: Downloads OI from API, saves to cache
- Second run: Loads OI from cache (no API call)
- Single-day downloads: Skip expensive API date discovery
"""
from console_log import log_console

import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig, Task, DiscoverPolicy
from tdSynchManager.credentials import get_influx_credentials

# Load credentials
influx = get_influx_credentials()

cfg = ManagerConfig(
    root_dir="tests/data_opt",  # Separate directory for optimization tests
    max_concurrency=10,
    max_file_mb=16,
    overlap_seconds=60,
    influx_url=influx['url'],
    influx_bucket=influx['bucket'],
    influx_token=influx['token'],
    influx_org=None,
    influx_precision="nanosecond",
    influx_measure_prefix="",
    influx_write_batch=5000,
)


async def test_single_day_optimization():
    """
    Test 1: Verify single-day downloads skip API date fetch

    Expected log output:
    - "[API-DATES] Single-day download (YYYY-MM-DD), skipping API date fetch"
    - NO "[API-DATES] Fetching available dates..." message
    """
    log_console("\n" + "="*80)
    log_console("TEST 1: Single-Day Download Optimization (Skip API Date Fetch)")
    log_console("="*80)

    # Create task for single-day intraday download (today only)
    tasks = [
        Task(
            asset="option",
            symbols=["TLRY"],
            intervals=["5m"],
            sink="influxdb",
            enrich_bar_greeks=False,  # Disable greeks to focus on OI caching
            enrich_tick_greeks=False,
            first_date_override="20250102",  # Specific single day
            end_date_override="20250102",     # Same day = single-day download
            ignore_existing=False,
            discover_policy=DiscoverPolicy(mode="skip")
        ),
    ]

    log_console("\n[TEST] Running single-day download...")
    log_console("Expected: '[API-DATES] Single-day download' message")
    log_console("Expected: NO '[API-DATES] Fetching available dates' message\n")

    async with ThetaDataV3Client() as client:
        manager = ThetaSyncManager(cfg, client=client)
        await manager.run(tasks)

    log_console("\n[TEST] ✓ Single-day download completed")
    log_console("[TEST] Check logs above for '[API-DATES] Single-day download' message")


async def test_oi_cache():
    """
    Test 2: Verify OI caching works

    Downloads the same day twice:
    - First run: Should download OI from API and cache it
    - Second run: Should load OI from cache (no API call)

    Expected log output:
    - Run 1: "[OI-CACHE][SAVE] TLRY date=20250102 - Cached N OI records"
    - Run 2: "[OI-CACHE][HIT] Using cached OI for TLRY/20250102"
    """
    log_console("\n" + "="*80)
    log_console("TEST 2: OI Cache Optimization")
    log_console("="*80)

    tasks = [
        Task(
            asset="option",
            symbols=["TLRY"],
            intervals=["5m"],
            sink="influxdb",
            enrich_bar_greeks=False,
            enrich_tick_greeks=False,
            first_date_override="20250102",
            end_date_override="20250102",
            ignore_existing=False,
            discover_policy=DiscoverPolicy(mode="skip")
        ),
    ]

    # First download - should cache OI
    log_console("\n[TEST] === FIRST DOWNLOAD (expect cache MISS, API download) ===")
    log_console("Expected: '[OI-CACHE][SAVE]' message after OI download\n")

    async with ThetaDataV3Client() as client:
        manager = ThetaSyncManager(cfg, client=client)
        await manager.run(tasks)

    log_console("\n[TEST] First download completed")

    # Wait a moment
    await asyncio.sleep(2)

    # Second download - should use cached OI
    log_console("\n[TEST] === SECOND DOWNLOAD (expect cache HIT, no API call) ===")
    log_console("Expected: '[OI-CACHE][HIT] Using cached OI for TLRY/20250102' message")
    log_console("Expected: NO OI API download\n")

    async with ThetaDataV3Client() as client:
        manager = ThetaSyncManager(cfg, client=client)
        await manager.run(tasks)

    log_console("\n[TEST] ✓ OI cache test completed")
    log_console("[TEST] Check logs above for cache HIT on second download")


async def test_multi_day_still_fetches():
    """
    Test 3: Verify multi-day downloads STILL fetch API dates

    This ensures we didn't break the normal behavior for historical downloads.

    Expected log output:
    - "[API-DATES] Fetching available dates for TLRY (option/5m)..."
    - "[API-DATES] Found N available dates, iterating only those"
    """
    log_console("\n" + "="*80)
    log_console("TEST 3: Multi-Day Download (Should STILL Fetch API Dates)")
    log_console("="*80)

    tasks = [
        Task(
            asset="option",
            symbols=["TLRY"],
            intervals=["5m"],
            sink="influxdb",
            enrich_bar_greeks=False,
            enrich_tick_greeks=False,
            first_date_override="20241230",  # Multi-day range
            end_date_override="20250102",
            ignore_existing=False,
            discover_policy=DiscoverPolicy(mode="skip")
        ),
    ]

    log_console("\n[TEST] Running multi-day download...")
    log_console("Expected: '[API-DATES] Fetching available dates' message")
    log_console("Expected: '[API-DATES] Found N available dates' message\n")

    async with ThetaDataV3Client() as client:
        manager = ThetaSyncManager(cfg, client=client)
        await manager.run(tasks)

    log_console("\n[TEST] ✓ Multi-day download completed")
    log_console("[TEST] Check logs above for API date fetch messages")


async def main():
    """Run all optimization tests"""
    log_console("\n" + "="*80)
    log_console("INTRADAY PIPELINE OPTIMIZATION TEST SUITE")
    log_console("="*80)
    log_console("Testing:")
    log_console("  1. Single-day API date fetch skip")
    log_console("  2. OI caching (cache miss → cache hit)")
    log_console("  3. Multi-day still fetches dates (regression test)")

    try:
        # Test 1: Single-day optimization
        await test_single_day_optimization()

        # Test 2: OI cache
        await test_oi_cache()

        # Test 3: Multi-day regression
        await test_multi_day_still_fetches()

        log_console("\n" + "="*80)
        log_console("ALL OPTIMIZATION TESTS COMPLETED")
        log_console("="*80)
        log_console("\n✓ Review the logs above to verify:")
        log_console("  1. Single-day downloads skip API date fetch")
        log_console("  2. Second OI download uses cache (cache HIT)")
        log_console("  3. Multi-day downloads still fetch API dates")

    except Exception as e:
        log_console(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    asyncio.run(main())
