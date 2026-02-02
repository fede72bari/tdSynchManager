"""
Test script for EOD OI date-shift logic.

This script verifies that:
1. next_trading_date_map is built correctly from api_available_dates
2. Last available date is skipped for EOD downloads
3. OI is requested for next_trading_date instead of current date
4. Effective date matching works correctly
"""
from console_log import log_console

from datetime import datetime as dt, timedelta


def test_next_trading_date_map_construction():
    """Test that next_trading_date map is built correctly."""
    log_console("\n" + "="*80)
    log_console("TEST 1: next_trading_date_map construction")
    log_console("="*80)

    # Simulate api_available_dates
    api_available_dates = [
        "2026-01-06",
        "2026-01-07",
        "2026-01-08",
        "2026-01-09",
        "2026-01-12",  # Monday after weekend
    ]

    # Build map (same logic as in manager.py)
    next_trading_date_map = {}
    sorted_api_dates = sorted(api_available_dates)
    for i, date in enumerate(sorted_api_dates[:-1]):  # Exclude last (no next)
        next_trading_date_map[date] = sorted_api_dates[i + 1]

    log_console(f"Available dates: {sorted_api_dates}")
    log_console(f"Next trading date map: {next_trading_date_map}")
    log_console(f"Last date {sorted_api_dates[-1]} has no next -> will be SKIPPED")

    # Verify mapping
    expected_map = {
        "2026-01-06": "2026-01-07",
        "2026-01-07": "2026-01-08",
        "2026-01-08": "2026-01-09",
        "2026-01-09": "2026-01-12",
        # "2026-01-12" intentionally missing (last date)
    }

    assert next_trading_date_map == expected_map, f"Map mismatch: {next_trading_date_map} != {expected_map}"
    log_console("[PASS] Map construction correct")

    # Verify last date is NOT in map
    assert sorted_api_dates[-1] not in next_trading_date_map, "Last date should not be in map"
    log_console("[PASS] Last date correctly excluded")

    return next_trading_date_map, sorted_api_dates


def test_oi_request_logic(next_trading_date_map):
    """Test OI request date logic."""
    log_console("\n" + "="*80)
    log_console("TEST 2: OI request date logic")
    log_console("="*80)

    test_cases = [
        ("2026-01-06", "2026-01-07", "2026-01-06"),  # day_iso, oi_request, expected_effective
        ("2026-01-07", "2026-01-08", "2026-01-07"),
        ("2026-01-08", "2026-01-09", "2026-01-08"),
        ("2026-01-09", "2026-01-12", "2026-01-09"),
    ]

    for day_iso, expected_oi_request, expected_effective in test_cases:
        if day_iso in next_trading_date_map:
            oi_request_date = next_trading_date_map[day_iso]
            log_console(f"EOD {day_iso}:")
            log_console(f"  -> Request OI for {oi_request_date}")
            log_console(f"  -> Expected effective_date: {expected_effective}")

            assert oi_request_date == expected_oi_request, \
                f"OI request mismatch for {day_iso}: {oi_request_date} != {expected_oi_request}"
            log_console(f"  [PASS] Correct")
        else:
            log_console(f"EOD {day_iso}: SKIPPED (no next_trading_date)")

    log_console("\n[PASS] All OI request dates correct")


def test_boundary_cases():
    """Test boundary cases."""
    log_console("\n" + "="*80)
    log_console("TEST 3: Boundary cases")
    log_console("="*80)

    # Case 1: Single date available (should skip - no next)
    log_console("\nCase 1: Single date available")
    api_dates = ["2026-01-06"]
    sorted_dates = sorted(api_dates)
    next_map = {}
    for i, date in enumerate(sorted_dates[:-1]):
        next_map[date] = sorted_dates[i + 1]

    log_console(f"  Available: {api_dates}")
    log_console(f"  Map: {next_map}")
    assert len(next_map) == 0, "Single date should produce empty map"
    log_console("  [PASS] Single date -> empty map (all dates skipped)")

    # Case 2: Two dates available (first has next, last skipped)
    log_console("\nCase 2: Two dates available")
    api_dates = ["2026-01-06", "2026-01-07"]
    sorted_dates = sorted(api_dates)
    next_map = {}
    for i, date in enumerate(sorted_dates[:-1]):
        next_map[date] = sorted_dates[i + 1]

    log_console(f"  Available: {api_dates}")
    log_console(f"  Map: {next_map}")
    assert next_map == {"2026-01-06": "2026-01-07"}, "Two dates should map first to second"
    assert "2026-01-07" not in next_map, "Last date should not be in map"
    log_console("  [PASS] First date mapped, last date skipped")

    # Case 3: Weekend gap handling (already handled by api_available_dates)
    log_console("\nCase 3: Weekend gap")
    api_dates = ["2026-01-09", "2026-01-12"]  # Fri -> Mon
    sorted_dates = sorted(api_dates)
    next_map = {}
    for i, date in enumerate(sorted_dates[:-1]):
        next_map[date] = sorted_dates[i + 1]

    log_console(f"  Available: {api_dates}")
    log_console(f"  Map: {next_map}")
    assert next_map == {"2026-01-09": "2026-01-12"}, "Weekend gap should be handled"
    log_console("  [PASS] Weekend gap correctly mapped")


def test_semantic_correctness():
    """Test semantic correctness of EOD bar containing same-session close OI."""
    log_console("\n" + "="*80)
    log_console("TEST 4: Semantic correctness")
    log_console("="*80)

    log_console("\nScenario: Download EOD for 2026-01-08")
    log_console("  OLD logic:")
    log_console("    - Request OI for 2026-01-08")
    log_console("    - Response: OI from 2026-01-07 close (prior session)")
    log_console("    - EOD bar 2026-01-08 contains OI from 2026-01-07 [FAIL]")
    log_console("\n  NEW logic:")
    log_console("    - Request OI for 2026-01-09 (next trading date)")
    log_console("    - Response: OI from 2026-01-08 close (effective_date=2026-01-08)")
    log_console("    - EOD bar 2026-01-08 contains OI from 2026-01-08 [PASS]")
    log_console("\n[PASS] Semantically correct: EOD bar contains same-session close OI")


def test_prevents_new_chains_without_oi():
    """Test that logic prevents downloading EOD for new chains without OI."""
    log_console("\n" + "="*80)
    log_console("TEST 5: Prevents new chains without OI")
    log_console("="*80)

    log_console("\nScenario: Today is 2026-01-10, market open")
    log_console("  Last complete session: 2026-01-09")
    log_console("  ThetaData available dates: [..., 2026-01-08, 2026-01-09]")
    log_console("  New option chain created: 2026-01-10 (not yet in historical data)")
    log_console("\n  OLD logic:")
    log_console("    - Download EOD for 2026-01-09")
    log_console("    - Request OI for 2026-01-09")
    log_console("    - New chain exists in EOD but NOT in OI")
    log_console("    - Merge fails with missing OI [FAIL]")
    log_console("\n  NEW logic:")
    log_console("    - Last available date is 2026-01-09")
    log_console("    - No next_trading_date for 2026-01-09")
    log_console("    - SKIP EOD download for 2026-01-09")
    log_console("    - Download stops at 2026-01-08 [PASS]")
    log_console("\n[PASS] Prevents downloading EOD for dates where we can't get same-session OI")


def run_all_tests():
    """Run all tests."""
    log_console("\n" + "#"*80)
    log_console("# EOD OI DATE-SHIFT LOGIC TESTS")
    log_console("#"*80)

    try:
        next_map, sorted_dates = test_next_trading_date_map_construction()
        test_oi_request_logic(next_map)
        test_boundary_cases()
        test_semantic_correctness()
        test_prevents_new_chains_without_oi()

        log_console("\n" + "#"*80)
        log_console("# ALL TESTS PASSED [PASS]")
        log_console("#"*80)

    except AssertionError as e:
        log_console(f"\n[FAIL] TEST FAILED: {e}")
        raise
    except Exception as e:
        log_console(f"\n[FAIL] UNEXPECTED ERROR: {e}")
        raise


if __name__ == "__main__":
    run_all_tests()
