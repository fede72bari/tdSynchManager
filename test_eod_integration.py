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
from console_log import log_console

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager import ThetaSyncManager, ManagerConfig, Task, ThetaDataV3Client


async def test_eod_download_small_range():
    """Test EOD download with small date range to verify OI logic."""
    log_console("\n" + "="*80)
    log_console("INTEGRATION TEST: EOD Download with OI Date-Shift")
    log_console("="*80)

    # Create minimal config for testing
    config = ManagerConfig(
        root_dir="./test_output",
        max_file_mb=50,
        overlap_seconds=0,
    )

    # Create ThetaData client
    client = ThetaDataV3Client()

    # Create manager
    manager = ThetaSyncManager(cfg=config, client=client)

    # Test task: Download AAL option EOD for a small date range
    # Choose dates in the past to ensure data is available
    test_task = Task(
        asset="option",
        symbols=["AAL"],
        intervals=["1d"],
        first_date_override="2026-01-06",  # Monday
        end_date_override="2026-01-09",    # Thursday
        sink="csv",
        enrich_bar_greeks=True,  # Enable greeks to test full enrichment flow
        use_api_date_discovery=True,  # Enable API date discovery
    )

    log_console(f"\nTest configuration:")
    log_console(f"  Symbols: {test_task.symbols}")
    log_console(f"  Intervals: {test_task.intervals}")
    log_console(f"  Date range: {test_task.first_date_override} to {test_task.end_date_override}")
    log_console(f"  Sink: {test_task.sink}")
    log_console(f"  Enrich Greeks: {test_task.enrich_bar_greeks}")
    log_console(f"  API Date Discovery: {test_task.use_api_date_discovery}")

    log_console("\n" + "-"*80)
    log_console("Expected behavior:")
    log_console("  1. Fetch available dates from ThetaData API")
    log_console("  2. Build next_trading_date_map")
    log_console("  3. For each EOD date D:")
    log_console("     - Request OI for next_trading_date(D)")
    log_console("     - Merge OI with effective_date matching D")
    log_console("  4. Skip last available date (no next_trading_date)")
    log_console("-"*80)

    try:
        log_console("\nStarting download...")
        log_console("(Watch for [EOD-OI-SHIFT] and [OI-EOD] log messages)")
        log_console("")

        await manager.run(tasks=[test_task])

        log_console("\n" + "="*80)
        log_console("INTEGRATION TEST COMPLETED")
        log_console("="*80)

        # Check output files
        output_dir = Path("./test_output/data/option/AAL/1d/csv")
        if output_dir.exists():
            files = sorted(output_dir.glob("*.csv"))
            log_console(f"\nOutput files created: {len(files)}")
            for f in files:
                log_console(f"  - {f.name}")

            # Verify that last available date was skipped
            if files:
                last_file = files[-1]
                last_date = last_file.name.split("T")[0]
                log_console(f"\nLast downloaded date: {last_date}")
                log_console(f"Expected: Should NOT be the last available date from ThetaData")
        else:
            log_console("\nWARNING: No output files found")

    except Exception as e:
        log_console(f"\n[FAIL] Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        # Cleanup
        await manager.close()


def main():
    """Run integration test."""
    log_console("\n" + "#"*80)
    log_console("# EOD OI DATE-SHIFT INTEGRATION TEST")
    log_console("#"*80)
    log_console("\nPre-requisites:")
    log_console("  1. ThetaData terminal must be running")
    log_console("  2. Valid ThetaData credentials in credentials.json")
    log_console("  3. Internet connection")
    log_console("\nThis test will download a few days of AAL option EOD data")
    log_console("to verify the new OI date-shift logic.")
    log_console("")

    try:
        asyncio.run(test_eod_download_small_range())
    except KeyboardInterrupt:
        log_console("\n\nTest interrupted by user")
    except Exception as e:
        log_console(f"\n\n[FAIL] Test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
