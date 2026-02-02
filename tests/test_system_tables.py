#!/usr/bin/env python3
"""
Test to discover ALL available system tables in InfluxDB v3.

We want to find if there's a system table for WAL metadata.
"""
from console_log import log_console

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from tdSynchManager.credentials import get_influx_credentials
from influxdb_client_3 import InfluxDBClient3
import pandas as pd


def discover_system_tables():
    """Discover all system tables and their schemas."""

    influx = get_influx_credentials()
    cli = InfluxDBClient3(
        host=influx.get('url', 'http://127.0.0.1:8181'),
        token=influx['token'],
        database=influx.get('bucket', 'ThetaData')
    )

    log_console("=" * 100)
    log_console("DISCOVERING ALL INFLUXDB v3 SYSTEM TABLES")
    log_console("=" * 100)

    # Known system tables to try
    known_tables = [
        "system.queries",
        "system.tables",
        "system.partitions",
        "system.compactor",
        "system.parquet_files",
        "system.wal",           # Try this
        "system.wal_files",     # Try this
        "system.metadata",      # Try this
        "system.cache",         # Try this
        "system.statistics",    # Try this
    ]

    available_tables = []

    for table in known_tables:
        log_console(f"\n[TEST] Checking {table}...")
        try:
            # Try to get schema
            query = f"SELECT * FROM {table} LIMIT 1"
            result = cli.query(query)
            df = result.to_pandas() if hasattr(result, "to_pandas") else result

            if df is not None:
                columns = df.columns.tolist() if hasattr(df, 'columns') else []
                row_count = len(df)
                available_tables.append(table)
                log_console(f"  [OK] Table exists!")
                log_console(f"  Columns: {columns}")
                log_console(f"  Sample rows: {row_count}")

                # Show sample data
                if not df.empty:
                    log_console(f"  Sample row:")
                    for col, val in df.iloc[0].items():
                        log_console(f"    {col}: {val}")
            else:
                log_console(f"  [EMPTY] Table exists but is empty")
                available_tables.append(table)

        except Exception as e:
            error_msg = str(e).lower()
            if "not found" in error_msg or "unknown table" in error_msg or "does not exist" in error_msg:
                log_console(f"  [NOT FOUND] Table does not exist")
            else:
                log_console(f"  [ERROR] {e}")

    # Try to discover tables using information_schema if available
    log_console(f"\n\n[DISCOVERY] Trying to list all system tables via information_schema...")
    try:
        discovery_queries = [
            "SHOW TABLES",
            "SELECT * FROM information_schema.tables WHERE table_schema = 'system'",
            "SELECT table_name FROM information_schema.tables",
        ]

        for query in discovery_queries:
            log_console(f"\n  Trying: {query}")
            try:
                result = cli.query(query)
                df = result.to_pandas() if hasattr(result, "to_pandas") else result
                if df is not None and not df.empty:
                    log_console(f"  [SUCCESS] Found tables:")
                    log_console(df)
                    break
            except Exception as e:
                log_console(f"  [FAILED] {e}")
    except Exception as e:
        log_console(f"  [ERROR] Discovery failed: {e}")

    log_console("\n" + "=" * 100)
    log_console(f"SUMMARY: Found {len(available_tables)} system tables")
    log_console("=" * 100)
    for table in available_tables:
        log_console(f"  - {table}")

    # Now test if we can get WAL metadata for specific measurement
    log_console("\n" + "=" * 100)
    log_console("TESTING: Can we get metadata for WAL-only data?")
    log_console("=" * 100)

    test_meas = "QQQ-option-5m"  # We know this has 0 Parquet files
    log_console(f"\nMeasurement: {test_meas} (known to have 0 Parquet files)")

    # Check system.tables for this measurement
    if "system.tables" in available_tables:
        log_console(f"\n[TEST] Querying system.tables for '{test_meas}'...")
        try:
            query = f"SELECT * FROM system.tables WHERE table_name = '{test_meas}'"
            result = cli.query(query)
            df = result.to_pandas() if hasattr(result, "to_pandas") else result
            if df is not None and not df.empty:
                log_console(f"  [OK] Found metadata in system.tables:")
                for col in df.columns:
                    log_console(f"    {col}: {df[col].iloc[0]}")
            else:
                log_console(f"  [EMPTY] No metadata found in system.tables")
        except Exception as e:
            log_console(f"  [ERROR] {e}")

    # Check system.partitions
    if "system.partitions" in available_tables:
        log_console(f"\n[TEST] Querying system.partitions for '{test_meas}'...")
        try:
            query = f"SELECT * FROM system.partitions WHERE table_name = '{test_meas}' LIMIT 5"
            result = cli.query(query)
            df = result.to_pandas() if hasattr(result, "to_pandas") else result
            if df is not None and not df.empty:
                log_console(f"  [OK] Found {len(df)} partitions:")
                log_console(f"  Columns: {df.columns.tolist()}")
                # Check if there are min_time/max_time columns
                for col in ['min_time', 'max_time', 'min', 'max']:
                    if col in df.columns:
                        log_console(f"  {col} present: YES")
                        log_console(f"    Sample values: {df[col].head()}")
            else:
                log_console(f"  [EMPTY] No partitions found")
        except Exception as e:
            log_console(f"  [ERROR] {e}")

    log_console("\n" + "=" * 100)
    log_console("DISCOVERY COMPLETE")
    log_console("=" * 100)


if __name__ == "__main__":
    discover_system_tables()
