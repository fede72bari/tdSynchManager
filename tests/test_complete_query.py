#!/usr/bin/env python3
"""
Complete test suite for query_local_data() with all parameter combinations.
Tests all sinks (CSV, Parquet, InfluxDB) and all get_* parameters.
"""

import os
import sys
import pandas as pd
import asyncio
from datetime import datetime

sys.path.insert(0, 'src')

from tdSynchManager.config import ManagerConfig
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.client import ThetaDataV3Client

async def main():
    """Main test function."""
    print("=" * 80)
    print("COMPLETE QUERY TEST SUITE")
    print("=" * 80)

    # Create manager with InfluxDB config
    cfg = ManagerConfig(
        root_dir="./data",
        max_concurrency=5,
        influx_url="http://127.0.0.1:8181",
        influx_bucket="ThetaData",
        influx_token="apiv3_WUNxFGW5CsII-ZTTME1Q4Bycq4DgsUksWwgEuSPZlb1WXdWT5TDyxvHEosashE7Um_bvWSkxaqNmq2ejGGDoZQ"
    )

    async with ThetaDataV3Client(base_url="http://localhost:25503/v3") as client:
        manager = ThetaSyncManager(cfg, client)

# Get available data
print("\nScanning available data...")
available = manager.list_available_data()
print(f"Found {len(available)} data series\n")

# Group by sink
sinks = available.groupby('sink')

test_results = []

def run_test(name, asset, symbol, interval, sink, start_date, **kwargs):
    """Run a single test and return results."""
    print(f"\n{'='*80}")
    print(f"TEST: {name}")
    print(f"{'='*80}")
    print(f"  Asset: {asset}, Symbol: {symbol}, Interval: {interval}, Sink: {sink}")
    print(f"  Start: {start_date}")
    print(f"  Params: {kwargs}")

    try:
        df, warnings = manager.query_local_data(
            asset=asset,
            symbol=symbol,
            interval=interval,
            sink=sink,
            start_date=start_date,
            **kwargs
        )

        if warnings:
            print(f"  ⚠ Warnings: {warnings}")

        if df is not None and len(df) > 0:
            print(f"  ✓ SUCCESS: {len(df)} rows returned")

            # Check ordering
            if 'timestamp' in df.columns:
                print(f"  Timestamp range: {df['timestamp'].min()} to {df['timestamp'].max()}")

            # For options, check if ordered correctly
            if asset == "option" and len(df) > 1:
                if 'expiration' in df.columns and 'strike' in df.columns:
                    # Check if sorted by expiration, strike, timestamp desc
                    is_sorted = True
                    for i in range(len(df) - 1):
                        exp1, exp2 = df.iloc[i]['expiration'], df.iloc[i+1]['expiration']
                        strike1, strike2 = df.iloc[i]['strike'], df.iloc[i+1]['strike']
                        if exp1 > exp2:
                            is_sorted = False
                            break
                        elif exp1 == exp2 and strike1 > strike2:
                            is_sorted = False
                            break
                    print(f"  Ordering check (exp, strike, ts DESC): {'✓ PASS' if is_sorted else '✗ FAIL'}")

            # For non-options, check timestamp DESC
            elif 'timestamp' in df.columns and len(df) > 1:
                timestamps = pd.to_datetime(df['timestamp'])
                is_desc = all(timestamps.iloc[i] >= timestamps.iloc[i+1] for i in range(len(timestamps)-1))
                print(f"  Ordering check (ts DESC): {'✓ PASS' if is_desc else '✗ FAIL'}")

            # Show sample
            print(f"\n  First 3 rows:")
            if 'expiration' in df.columns and 'strike' in df.columns:
                cols = ['expiration', 'strike', 'timestamp'] if 'timestamp' in df.columns else ['expiration', 'strike']
                print(df[cols].head(3).to_string(index=False))
            elif 'timestamp' in df.columns:
                print(df[['timestamp']].head(3).to_string(index=False))

            return {'test': name, 'status': 'PASS', 'rows': len(df), 'warnings': len(warnings)}
        else:
            print(f"  ✗ FAIL: No data returned")
            return {'test': name, 'status': 'FAIL', 'rows': 0, 'warnings': len(warnings)}

    except Exception as e:
        print(f"  ✗ ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return {'test': name, 'status': 'ERROR', 'rows': 0, 'error': str(e)}


# ============================================================================
# TEST 1: Basic query without get_* parameters (CSV)
# ============================================================================
if not available[available['sink'] == 'csv'].empty:
    row = available[available['sink'] == 'csv'].iloc[0]
    start_date = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

    result = run_test(
        "Basic Query (CSV)",
        row['asset'], row['symbol'], row['interval'], 'csv',
        start_date,
        max_rows=10
    )
    test_results.append(result)

# ============================================================================
# TEST 2: get_first_n_rows (Parquet)
# ============================================================================
if not available[available['sink'] == 'parquet'].empty:
    row = available[available['sink'] == 'parquet'].iloc[0]
    start_date = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

    result = run_test(
        "get_first_n_rows=5 (Parquet)",
        row['asset'], row['symbol'], row['interval'], 'parquet',
        start_date,
        get_first_n_rows=5
    )
    test_results.append(result)

# ============================================================================
# TEST 3: get_last_n_rows (InfluxDB)
# ============================================================================
if not available[available['sink'] == 'influxdb'].empty:
    row = available[available['sink'] == 'influxdb'].iloc[0]
    start_date = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

    result = run_test(
        "get_last_n_rows=5 (InfluxDB)",
        row['asset'], row['symbol'], row['interval'], 'influxdb',
        start_date,
        get_last_n_rows=5
    )
    test_results.append(result)

# ============================================================================
# TEST 4: get_first_n_days (CSV)
# ============================================================================
if not available[available['sink'] == 'csv'].empty:
    row = available[available['sink'] == 'csv'].iloc[0]
    start_date = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

    result = run_test(
        "get_first_n_days=1 (CSV)",
        row['asset'], row['symbol'], row['interval'], 'csv',
        start_date,
        get_first_n_days=1
    )
    test_results.append(result)

# ============================================================================
# TEST 5: get_last_n_days without start_date (Parquet)
# ============================================================================
if not available[available['sink'] == 'parquet'].empty:
    row = available[available['sink'] == 'parquet'].iloc[0]

    result = run_test(
        "get_last_n_days=1 (no start_date) (Parquet)",
        row['asset'], row['symbol'], row['interval'], 'parquet',
        None,
        get_last_n_days=1
    )
    test_results.append(result)

# ============================================================================
# TEST 6: get_first_n_minutes (InfluxDB)
# ============================================================================
if not available[available['sink'] == 'influxdb'].empty:
    row = available[available['sink'] == 'influxdb'].iloc[0]
    start_date = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

    result = run_test(
        "get_first_n_minutes=30 (InfluxDB)",
        row['asset'], row['symbol'], row['interval'], 'influxdb',
        start_date,
        get_first_n_minutes=30
    )
    test_results.append(result)

# ============================================================================
# TEST 7: get_last_n_minutes (CSV)
# ============================================================================
if not available[(available['sink'] == 'csv') & (available['interval'] == 'tick')].empty:
    row = available[(available['sink'] == 'csv') & (available['interval'] == 'tick')].iloc[0]
    start_date = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

    result = run_test(
        "get_last_n_minutes=10 (CSV tick)",
        row['asset'], row['symbol'], row['interval'], 'csv',
        start_date,
        get_last_n_minutes=10
    )
    test_results.append(result)

# ============================================================================
# TEST 8: Multiple get_* parameters (should FAIL with warning)
# ============================================================================
if not available[available['sink'] == 'csv'].empty:
    row = available[available['sink'] == 'csv'].iloc[0]
    start_date = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

    result = run_test(
        "MULTIPLE get_* params (should fail)",
        row['asset'], row['symbol'], row['interval'], 'csv',
        start_date,
        get_first_n_rows=5,
        get_last_n_days=1  # Should trigger warning
    )
    test_results.append(result)

# ============================================================================
# TEST 9: Options ordering check (CSV)
# ============================================================================
if not available[(available['sink'] == 'csv') & (available['asset'] == 'option')].empty:
    row = available[(available['sink'] == 'csv') & (available['asset'] == 'option')].iloc[0]
    start_date = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

    result = run_test(
        "Options Ordering (exp, strike, ts DESC)",
        row['asset'], row['symbol'], row['interval'], 'csv',
        start_date,
        max_rows=20
    )
    test_results.append(result)

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("TEST SUMMARY")
print("=" * 80)

df_results = pd.DataFrame(test_results)
print(df_results.to_string(index=False))

passed = len([r for r in test_results if r['status'] == 'PASS'])
failed = len([r for r in test_results if r['status'] == 'FAIL'])
errors = len([r for r in test_results if r['status'] == 'ERROR'])

print(f"\nTotal tests: {len(test_results)}")
print(f"  ✓ Passed: {passed}")
print(f"  ✗ Failed: {failed}")
print(f"  ⚠ Errors: {errors}")
print(f"\nSuccess rate: {100 * passed / len(test_results):.1f}%")
