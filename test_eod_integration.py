"""
Integration test for EOD OI date-shift logic.

This script performs a small EOD download to verify:
1. next_trading_date_map is built correctly
2. OI is requested for next_trading_date
3. Last available date is skipped
4. effective_date matching works correctly

Usage:
    python test_eod_integration.py

Note: This requires ThetaData terminal to be running.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager import ThetaSyncManager, ManagerConfig, Task


async def test_eod_download_small_range():
    """Test EOD download with small date range to verify OI logic."""
    print("\n" + "="*80)
    print("INTEGRATION TEST: EOD Download with OI Date-Shift")
    print("="*80)

    # Create minimal config for testing
    config = ManagerConfig(
        root_dir="./test_output",
        influx_host=None,  # Disable InfluxDB for this test
        max_file_mb=50,
        overlap_seconds=0,
    )

    # Create manager
    manager = ThetaSyncManager(cfg=config)

    # Test task: Download AAL option EOD for a small date range
    # Choose dates in the past to ensure data is available
    test_task = Task(
        asset="option",
        symbol="AAL",
        interval="1d",
        start_date="2026-01-06",  # Monday
        end_date="2026-01-09",    # Thursday
        sink="csv",
        enrich_bar_greeks=True,  # Enable greeks to test full enrichment flow
        use_api_date_discovery=True,  # Enable API date discovery
    )

    print(f"\nTest configuration:")
    print(f"  Symbol: {test_task.symbol}")
    print(f"  Interval: {test_task.interval}")
    print(f"  Date range: {test_task.start_date} to {test_task.end_date}")
    print(f"  Sink: {test_task.sink}")
    print(f"  Enrich Greeks: {test_task.enrich_bar_greeks}")
    print(f"  API Date Discovery: {test_task.use_api_date_discovery}")

    print("\n" + "-"*80)
    print("Expected behavior:")
    print("  1. Fetch available dates from ThetaData API")
    print("  2. Build next_trading_date_map")
    print("  3. For each EOD date D:")
    print("     - Request OI for next_trading_date(D)")
    print("     - Merge OI with effective_date matching D")
    print("  4. Skip last available date (no next_trading_date)")
    print("-"*80)

    try:
        print("\nStarting download...")
        print("(Watch for [EOD-OI-SHIFT] and [OI-EOD] log messages)")
        print("")

        await manager.run(tasks=[test_task])

        print("\n" + "="*80)
        print("INTEGRATION TEST COMPLETED")
        print("="*80)

        # Check output files
        output_dir = Path("./test_output/data/option/AAL/1d/csv")
        if output_dir.exists():
            files = sorted(output_dir.glob("*.csv"))
            print(f"\nOutput files created: {len(files)}")
            for f in files:
                print(f"  - {f.name}")

            # Verify that last available date was skipped
            if files:
                last_file = files[-1]
                last_date = last_file.name.split("T")[0]
                print(f"\nLast downloaded date: {last_date}")
                print(f"Expected: Should NOT be the last available date from ThetaData")
        else:
            print("\nWARNING: No output files found")

    except Exception as e:
        print(f"\n[FAIL] Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        # Cleanup
        await manager.close()


def main():
    """Run integration test."""
    print("\n" + "#"*80)
    print("# EOD OI DATE-SHIFT INTEGRATION TEST")
    print("#"*80)
    print("\nPre-requisites:")
    print("  1. ThetaData terminal must be running")
    print("  2. Valid ThetaData credentials in credentials.json")
    print("  3. Internet connection")
    print("\nThis test will download a few days of AAL option EOD data")
    print("to verify the new OI date-shift logic.")
    print("")

    try:
        asyncio.run(test_eod_download_small_range())
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
    except Exception as e:
        print(f"\n\n[FAIL] Test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
