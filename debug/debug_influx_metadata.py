#!/usr/bin/env python3
"""
Debug script to investigate InfluxDB v3 system.parquet_files metadata visibility.

Tests:
1. Check if system.parquet_files table exists and is accessible
2. List all tables in system.parquet_files
3. Check specific measurement metadata
4. Compare with direct SELECT on measurement
5. Try different query variations
"""

import sys
import os
from pathlib import Path

# Add parent to path to import tdSynchManager
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager.credentials import get_influx_credentials
from influxdb_client_3 import InfluxDBClient3
import pandas as pd


def debug_metadata():
    """Run debug queries on InfluxDB metadata."""

    # Get InfluxDB credentials
    influx = get_influx_credentials()
    influx_token = influx['token']
    influx_url = influx.get('url', 'http://127.0.0.1:8181')
    influx_bucket = influx.get('bucket', 'ThetaData')

    cli = InfluxDBClient3(
        host=influx_url,
        token=influx_token,
        database=influx_bucket
    )

    # Test measurement (adjust based on your data)
    test_meas = "QQQ-option-5m"  # Change this to match your actual measurement

    print("=" * 80)
    print("DEBUG: InfluxDB v3 Parquet Metadata Investigation")
    print("=" * 80)

    # TEST 1: Check if system.parquet_files exists
    print("\n[TEST 1] Checking if system.parquet_files exists...")
    try:
        test_query = "SELECT COUNT(*) as cnt FROM system.parquet_files"
        result = cli.query(test_query)
        df = result.to_pandas() if hasattr(result, "to_pandas") else result
        if df is not None and not df.empty:
            print(f"[OK] system.parquet_files exists, has {df['cnt'].iloc[0]} rows")
        else:
            print("[EMPTY] system.parquet_files exists but is empty")
    except Exception as e:
        print(f"[ERROR] Failed to query system.parquet_files: {e}")
        return

    # TEST 2: List all tables in system.parquet_files
    print("\n[TEST 2] Listing all tables in system.parquet_files...")
    try:
        list_query = "SELECT DISTINCT table_name FROM system.parquet_files"
        result = cli.query(list_query)
        df = result.to_pandas() if hasattr(result, "to_pandas") else result
        if df is not None and not df.empty:
            tables = df['table_name'].tolist()
            print(f"[OK] Found {len(tables)} tables:")
            for i, table in enumerate(tables[:10], 1):
                marker = "  <- TARGET" if table == test_meas else ""
                print(f"  {i}. {table}{marker}")
            if len(tables) > 10:
                print(f"  ... and {len(tables) - 10} more")
        else:
            print("[ERROR] No tables found in system.parquet_files")
            return
    except Exception as e:
        print(f"[ERROR] Failed to list tables: {e}")
        return

    # TEST 3: Check if target measurement has Parquet files
    print(f"\n[TEST 3] Checking Parquet files for '{test_meas}'...")
    try:
        check_query = f"""
        SELECT COUNT(*) as file_count,
               MIN(min_time) as earliest,
               MAX(max_time) as latest
        FROM system.parquet_files
        WHERE table_name = '{test_meas}'
        """
        result = cli.query(check_query)
        df = result.to_pandas() if hasattr(result, "to_pandas") else result
        if df is not None and not df.empty:
            row = df.iloc[0]
            file_count = row['file_count']
            print(f"  File count: {file_count}")
            if file_count > 0:
                earliest = pd.Timestamp(row['earliest'], unit='ns', tz='UTC') if pd.notna(row['earliest']) else None
                latest = pd.Timestamp(row['latest'], unit='ns', tz='UTC') if pd.notna(row['latest']) else None
                print(f"  Earliest: {earliest}")
                print(f"  Latest: {latest}")
            else:
                print(f"[ERROR] No Parquet files found for '{test_meas}'")
        else:
            print("[ERROR] Query returned empty")
    except Exception as e:
        print(f"[ERROR] Failed to check Parquet files: {e}")

    # TEST 4: Direct SELECT on measurement
    print(f"\n[TEST 4] Direct SELECT MIN/MAX(time) on '{test_meas}'...")
    try:
        direct_query = f'SELECT MIN(time) as min_t, MAX(time) as max_t FROM "{test_meas}"'
        result = cli.query(direct_query)
        df = result.to_pandas() if hasattr(result, "to_pandas") else result
        if df is not None and not df.empty:
            row = df.iloc[0]
            if pd.notna(row['min_t']) and pd.notna(row['max_t']):
                min_t = pd.to_datetime(row['min_t'], utc=True)
                max_t = pd.to_datetime(row['max_t'], utc=True)
                print(f"[OK] Data exists in measurement:")
                print(f"  Min time: {min_t}")
                print(f"  Max time: {max_t}")
            else:
                print(f"[ERROR] Measurement exists but has no data")
        else:
            print(f"[ERROR] Measurement '{test_meas}' appears empty")
    except Exception as e:
        print(f"[ERROR] Failed to query measurement: {e}")

    # TEST 5: Show schema of system.parquet_files
    print(f"\n[TEST 5] Schema of system.parquet_files...")
    try:
        schema_query = "SELECT * FROM system.parquet_files LIMIT 1"
        result = cli.query(schema_query)
        df = result.to_pandas() if hasattr(result, "to_pandas") else result
        if df is not None and not df.empty:
            print(f"[OK] Columns: {df.columns.tolist()}")
            print(f"  Sample row:")
            for col, val in df.iloc[0].items():
                print(f"    {col}: {val}")
        else:
            print("[ERROR] Could not get schema (empty result)")
    except Exception as e:
        print(f"[ERROR] Failed to get schema: {e}")

    print("\n" + "=" * 80)
    print("DEBUG COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    debug_metadata()
