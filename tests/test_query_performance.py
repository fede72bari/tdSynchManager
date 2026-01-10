#!/usr/bin/env python3
"""
Performance test: Compare query times for MIN/MAX retrieval methods.

Tests:
1. Query system.parquet_files metadata (KB of data)
2. Direct SELECT MIN/MAX on measurement (data scan or metadata?)
3. Measure time difference to understand if SELECT uses metadata

Theory:
- If SELECT MIN/MAX is SLOW (>100ms on large table), it scans data
- If SELECT MIN/MAX is FAST (<10ms), it uses Parquet metadata
"""

import sys
import os
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager.credentials import get_influx_credentials
from influxdb_client_3 import InfluxDBClient3
import pandas as pd


def benchmark_queries():
    """Run performance benchmark on different query methods."""

    # Get InfluxDB credentials
    influx = get_influx_credentials()
    cli = InfluxDBClient3(
        host=influx.get('url', 'http://127.0.0.1:8181'),
        token=influx['token'],
        database=influx.get('bucket', 'ThetaData')
    )

    # Test on tables with different characteristics
    test_cases = [
        ("QQQ-option-5m", "Recent data (likely in WAL only)"),
        ("XOM-option-5m", "Large table with Parquet files"),
        ("SPY-option-1d", "Older data (likely compacted)")
    ]

    print("=" * 100)
    print("PERFORMANCE BENCHMARK: Metadata Query vs Direct SELECT MIN/MAX")
    print("=" * 100)

    for meas, desc in test_cases:
        print(f"\n{'=' * 100}")
        print(f"Measurement: {meas}")
        print(f"Description: {desc}")
        print(f"{'=' * 100}\n")

        # TEST 1: Check if table has Parquet files
        print("[TEST 1] Checking Parquet file count...")
        try:
            count_query = f"SELECT COUNT(*) as cnt FROM system.parquet_files WHERE table_name = '{meas}'"
            result = cli.query(count_query)
            df = result.to_pandas() if hasattr(result, "to_pandas") else result
            file_count = df['cnt'].iloc[0] if df is not None and not df.empty else 0
            print(f"  Parquet files: {file_count}")
        except Exception as e:
            print(f"  ERROR: {e}")
            file_count = 0

        # TEST 2: Query metadata from system.parquet_files
        print("\n[TEST 2] Query MIN/MAX from system.parquet_files metadata...")
        metadata_times = []
        metadata_result = None

        for i in range(5):  # Run 5 times to get average
            try:
                t0 = time.perf_counter()

                meta_query = f"""
                SELECT MIN(min_time) AS start_ts, MAX(max_time) AS end_ts
                FROM system.parquet_files
                WHERE table_name = '{meas}'
                """
                result = cli.query(meta_query)
                df = result.to_pandas() if hasattr(result, "to_pandas") else result

                elapsed = (time.perf_counter() - t0) * 1000  # Convert to ms
                metadata_times.append(elapsed)

                if i == 0 and df is not None and not df.empty:
                    row = df.iloc[0]
                    if pd.notna(row.get("start_ts")) and pd.notna(row.get("end_ts")):
                        min_ts = pd.Timestamp(row["start_ts"], unit='ns', tz='UTC')
                        max_ts = pd.Timestamp(row["end_ts"], unit='ns', tz='UTC')
                        metadata_result = (min_ts, max_ts)
            except Exception as e:
                print(f"  ERROR (run {i+1}): {e}")
                metadata_times.append(None)

        valid_meta_times = [t for t in metadata_times if t is not None]
        if valid_meta_times:
            avg_meta = sum(valid_meta_times) / len(valid_meta_times)
            min_meta = min(valid_meta_times)
            max_meta = max(valid_meta_times)
            print(f"  Times (ms): {', '.join(f'{t:.2f}' for t in valid_meta_times)}")
            print(f"  Average: {avg_meta:.2f} ms | Min: {min_meta:.2f} ms | Max: {max_meta:.2f} ms")
            if metadata_result:
                print(f"  Result: {metadata_result[0]} to {metadata_result[1]}")
        else:
            print(f"  No valid results (table likely has 0 Parquet files)")
            avg_meta = None

        # TEST 3: Direct SELECT MIN/MAX on measurement
        print("\n[TEST 3] Direct SELECT MIN(time), MAX(time) on measurement...")
        direct_times = []
        direct_result = None

        for i in range(5):  # Run 5 times to get average
            try:
                t0 = time.perf_counter()

                direct_query = f'SELECT MIN(time) AS min_t, MAX(time) AS max_t FROM "{meas}"'
                result = cli.query(direct_query)
                df = result.to_pandas() if hasattr(result, "to_pandas") else result

                elapsed = (time.perf_counter() - t0) * 1000  # Convert to ms
                direct_times.append(elapsed)

                if i == 0 and df is not None and not df.empty:
                    row = df.iloc[0]
                    if pd.notna(row.get("min_t")) and pd.notna(row.get("max_t")):
                        min_t = pd.to_datetime(row["min_t"], utc=True)
                        max_t = pd.to_datetime(row["max_t"], utc=True)
                        direct_result = (min_t, max_t)
            except Exception as e:
                print(f"  ERROR (run {i+1}): {e}")
                direct_times.append(None)

        valid_direct_times = [t for t in direct_times if t is not None]
        if valid_direct_times:
            avg_direct = sum(valid_direct_times) / len(valid_direct_times)
            min_direct = min(valid_direct_times)
            max_direct = max(valid_direct_times)
            print(f"  Times (ms): {', '.join(f'{t:.2f}' for t in valid_direct_times)}")
            print(f"  Average: {avg_direct:.2f} ms | Min: {min_direct:.2f} ms | Max: {max_direct:.2f} ms")
            if direct_result:
                print(f"  Result: {direct_result[0]} to {direct_result[1]}")
        else:
            print(f"  All queries failed")
            avg_direct = None

        # ANALYSIS
        print("\n[ANALYSIS]")
        print(f"  Parquet files: {file_count}")

        if avg_meta and avg_direct:
            speedup = avg_direct / avg_meta
            print(f"  Metadata query: {avg_meta:.2f} ms (avg)")
            print(f"  Direct query: {avg_direct:.2f} ms (avg)")
            print(f"  Speedup ratio: {speedup:.2f}x (metadata is {speedup:.1f}x faster)")

            if avg_direct < 20:
                print(f"  CONCLUSION: Direct SELECT is FAST (<20ms) -> Likely uses metadata!")
            elif avg_direct < 100:
                print(f"  CONCLUSION: Direct SELECT is MODERATE (20-100ms) -> May use metadata + some scan")
            else:
                print(f"  CONCLUSION: Direct SELECT is SLOW (>100ms) -> Likely scans data!")
        elif avg_direct and not avg_meta:
            print(f"  Metadata query: FAILED (no Parquet files)")
            print(f"  Direct query: {avg_direct:.2f} ms (avg)")
            if avg_direct < 20:
                print(f"  CONCLUSION: Direct SELECT is FAST even without Parquet -> Uses WAL metadata!")
            else:
                print(f"  CONCLUSION: Direct SELECT scans WAL data")
        else:
            print(f"  CONCLUSION: Unable to compare (one or both queries failed)")

        # Result consistency check
        if metadata_result and direct_result:
            if metadata_result[0] == direct_result[0] and metadata_result[1] == direct_result[1]:
                print(f"  Results: IDENTICAL (metadata == direct)")
            else:
                print(f"  Results: DIFFERENT!")
                print(f"    Metadata: {metadata_result[0]} to {metadata_result[1]}")
                print(f"    Direct:   {direct_result[0]} to {direct_result[1]}")
                print(f"    REASON: Direct query sees WAL data that's not in Parquet yet")

    print("\n" + "=" * 100)
    print("BENCHMARK COMPLETE")
    print("=" * 100)


if __name__ == "__main__":
    benchmark_queries()
