#!/usr/bin/env python3
"""
Extended tests for query_local_data() covering all date parameter combinations:
1. Only start_date (get_first_*)
2. Only end_date (get_last_*)
3. Both start_date and end_date (multi-file test)
4. Neither (get_last_* from end of data)
"""
from console_log import log_console

import sys
sys.path.insert(0, 'src')

import pandas as pd
import asyncio
from tdSynchManager.config import ManagerConfig
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client


def display_result(title, df, warnings):
    """Display query result."""
    log_console("\n" + "=" * 80)
    log_console(title)
    log_console("=" * 80)

    if warnings:
        log_console(f"Warnings: {warnings}")

    if df is None or len(df) == 0:
        log_console("FAIL: No data returned")
        return False

    log_console(f"OK: {len(df)} rows")

    # Show time range
    time_col = None
    for col in ['timestamp', 'ms_of_day', 'date']:
        if col in df.columns:
            time_col = col
            break

    if time_col:
        df_copy = df.copy()
        df_copy[time_col] = pd.to_datetime(df_copy[time_col])
        time_min = df_copy[time_col].min()
        time_max = df_copy[time_col].max()
        log_console(f"Time range: {time_min} to {time_max}")
        log_console(f"Span: {time_max - time_min}")

        # Show first and last timestamp
        log_console(f"\nFirst timestamp: {time_min}")
        log_console(f"Last timestamp: {time_max}")

    return True


async def main():
    """Main test function."""
    log_console("=" * 80)
    log_console("EXTENDED QUERY TESTS - ALL DATE PARAMETER COMBINATIONS")
    log_console("=" * 80)

    # Create manager
    cfg = ManagerConfig(
        root_dir=r'tests/data',
        max_concurrency=5,
        influx_url='http://127.0.0.1:8181',
        influx_bucket='ThetaData',
        influx_token='apiv3_WUNxFGW5CsII-ZTTME1Q4Bycq4DgsUksWwgEuSPZlb1WXdWT5TDyxvHEosashE7Um_bvWSkxaqNmq2ejGGDoZQ'
    )

    async with ThetaDataV3Client(base_url='http://localhost:25503/v3') as client:
        manager = ThetaSyncManager(cfg, client)

        # Get available data
        log_console("\nScanning available data...")
        available = manager.list_available_data()
        log_console(f"Found {len(available)} data series\n")

        # Test on first available series (should be a CSV with multiple days of data)
        if available.empty:
            log_console("No data available for testing")
            return

        # Find a series with multi-day data
        test_series = None
        for idx, row in available.iterrows():
            first_dt = pd.to_datetime(row['first_datetime'])
            last_dt = pd.to_datetime(row['last_datetime'])

            # Remove timezone if present for comparison
            if hasattr(first_dt, 'tz') and first_dt.tz is not None:
                first_dt = first_dt.tz_localize(None)
            if hasattr(last_dt, 'tz') and last_dt.tz is not None:
                last_dt = last_dt.tz_localize(None)

            try:
                days_span = (last_dt - first_dt).days
            except TypeError:
                continue  # Skip this row if datetime types are incompatible

            if days_span >= 2:  # At least 2 days of data
                test_series = row
                break

        if test_series is None:
            log_console("No multi-day series found for testing")
            return

        asset = test_series['asset']
        symbol = test_series['symbol']
        interval = test_series['interval']
        sink = test_series['sink']

        first_dt = pd.to_datetime(test_series['first_datetime'])
        last_dt = pd.to_datetime(test_series['last_datetime'])

        # Remove timezone if present
        if first_dt.tz is not None:
            first_dt = first_dt.tz_localize(None)
        if last_dt.tz is not None:
            last_dt = last_dt.tz_localize(None)

        mid_dt = first_dt + (last_dt - first_dt) / 2

        log_console(f"Testing with: {symbol} ({asset}) - {interval} - {sink}")
        log_console(f"Date range: {first_dt.date()} to {last_dt.date()}")
        log_console(f"Middle date: {mid_dt.date()}")

        results = []

        # =====================================================================
        # TEST 1: get_first_n_rows=10 with ONLY start_date
        # =====================================================================
        log_console("\n" + "=" * 80)
        log_console("TEST 1: get_first_n_rows=10 with ONLY start_date")
        log_console("Expected: First 10 rows chronologically from start_date")
        log_console("=" * 80)

        df, warn = manager.query_local_data(
            asset=asset, symbol=symbol, interval=interval, sink=sink,
            start_date=first_dt.strftime("%Y-%m-%d"),
            get_first_n_rows=10
        )

        success = display_result(
            f"get_first_n_rows=10 with start_date={first_dt.date()}",
            df, warn
        )
        results.append(('TEST 1', 'PASS' if success else 'FAIL'))

        # =====================================================================
        # TEST 2: get_last_n_rows=10 with ONLY start_date
        # =====================================================================
        log_console("\n" + "=" * 80)
        log_console("TEST 2: get_last_n_rows=10 with ONLY start_date")
        log_console("Expected: Last 10 rows chronologically (most recent from all data)")
        log_console("=" * 80)

        df, warn = manager.query_local_data(
            asset=asset, symbol=symbol, interval=interval, sink=sink,
            start_date=first_dt.strftime("%Y-%m-%d"),
            get_last_n_rows=10
        )

        success = display_result(
            f"get_last_n_rows=10 with start_date={first_dt.date()}",
            df, warn
        )

        # Verify that last timestamp is close to last_dt (within 3 days)
        if success and df is not None and len(df) > 0:
            max_ts = pd.to_datetime(df['timestamp']).max()
            days_diff = abs((last_dt - max_ts).days)
            success = days_diff <= 3
            if not success:
                log_console(f"FAIL: Last timestamp {max_ts.date()} is {days_diff} days away from expected {last_dt.date()}")

        results.append(('TEST 2', 'PASS' if success else 'FAIL'))

        # =====================================================================
        # TEST 3: get_last_n_rows=10 with NO start_date (from end)
        # =====================================================================
        log_console("\n" + "=" * 80)
        log_console("TEST 3: get_last_n_rows=10 with NO start_date")
        log_console("Expected: Last 10 rows from end of all data")
        log_console("=" * 80)

        df, warn = manager.query_local_data(
            asset=asset, symbol=symbol, interval=interval, sink=sink,
            get_last_n_rows=10
        )

        success = display_result(
            "get_last_n_rows=10 (no start_date)",
            df, warn
        )
        results.append(('TEST 3', 'PASS' if success else 'FAIL'))

        # =====================================================================
        # TEST 4: Query with BOTH start_date and end_date (multi-file)
        # =====================================================================
        log_console("\n" + "=" * 80)
        log_console("TEST 4: Query with BOTH start_date and end_date")
        log_console("Expected: All data between the two dates")
        log_console("=" * 80)

        df, warn = manager.query_local_data(
            asset=asset, symbol=symbol, interval=interval, sink=sink,
            start_date=first_dt.strftime("%Y-%m-%d"),
            end_date=mid_dt.strftime("%Y-%m-%d"),
            max_rows=20
        )

        success = display_result(
            f"start={first_dt.date()}, end={mid_dt.date()}, max_rows=20",
            df, warn
        )
        results.append(('TEST 4', 'PASS' if success else 'FAIL'))

        # =====================================================================
        # TEST 5: get_first_n_days=1 with start_date
        # =====================================================================
        log_console("\n" + "=" * 80)
        log_console("TEST 5: get_first_n_days=1 with start_date")
        log_console("Expected: First day of data from start_date")
        log_console("=" * 80)

        df, warn = manager.query_local_data(
            asset=asset, symbol=symbol, interval=interval, sink=sink,
            start_date=first_dt.strftime("%Y-%m-%d"),
            get_first_n_days=1
        )

        success = display_result(
            f"get_first_n_days=1 with start_date={first_dt.date()}",
            df, warn
        )

        if success:
            df_copy = df.copy()
            df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
            days_span = (df_copy['timestamp'].max() - df_copy['timestamp'].min()).days
            log_console(f"Actual day span: {days_span} (should be <= 1)")
            success = days_span <= 1

        results.append(('TEST 5', 'PASS' if success else 'FAIL'))

        # =====================================================================
        # TEST 6: get_last_n_days=1 with NO dates
        # =====================================================================
        log_console("\n" + "=" * 80)
        log_console("TEST 6: get_last_n_days=1 with NO dates")
        log_console("Expected: Last day of all available data")
        log_console("=" * 80)

        df, warn = manager.query_local_data(
            asset=asset, symbol=symbol, interval=interval, sink=sink,
            get_last_n_days=1
        )

        success = display_result(
            "get_last_n_days=1 (no dates)",
            df, warn
        )

        if success:
            df_copy = df.copy()
            df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
            last_ts = df_copy['timestamp'].max()
            log_console(f"Last timestamp: {last_ts} (should be near {last_dt})")
            success = (last_dt - last_ts).days <= 1

        results.append(('TEST 6', 'PASS' if success else 'FAIL'))

        # =====================================================================
        # SUMMARY
        # =====================================================================
        log_console("\n" + "=" * 80)
        log_console("TEST SUMMARY")
        log_console("=" * 80)

        for test_name, status in results:
            log_console(f"{test_name:30} {status}")

        passed = sum(1 for _, status in results if status == 'PASS')
        total = len(results)
        log_console(f"\nTotal: {total}, Passed: {passed}, Failed: {total - passed}")
        log_console(f"Success rate: {100 * passed / total:.1f}%")


if __name__ == "__main__":
    asyncio.run(main())
