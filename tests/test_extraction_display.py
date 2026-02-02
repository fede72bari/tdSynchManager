#!/usr/bin/env python3
"""
Comprehensive test of get_* parameters with display output.
Extracts 5 rows, 5 minutes, 2 days from beginning and end of all available databases.
"""
from console_log import log_console

import sys
sys.path.insert(0, 'src')

import pandas as pd
import asyncio
from IPython.display import display
from tdSynchManager.config import ManagerConfig
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client


def display_extraction(title, df, warnings):
    """Display extraction results with first and last rows."""
    log_console("\n" + "=" * 80)
    log_console(title)
    log_console("=" * 80)

    if warnings:
        log_console(f"WARNING: {warnings}")

    if df is None or len(df) == 0:
        log_console("FAIL: No data returned")
        return

    log_console(f"OK: Got {len(df)} rows")

    # Show time range
    if 'timestamp' in df.columns:
        df_copy = df.copy()
        df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
        time_min = df_copy['timestamp'].min()
        time_max = df_copy['timestamp'].max()
        time_span = time_max - time_min
        log_console(f"Time range: {time_min} to {time_max}")
        log_console(f"Span: {time_span}")

    # Display columns to show
    display_cols = []
    if 'timestamp' in df.columns:
        display_cols.append('timestamp')
    if 'expiration' in df.columns:
        display_cols.append('expiration')
    if 'strike' in df.columns:
        display_cols.append('strike')
    if 'open' in df.columns:
        display_cols.extend(['open', 'high', 'low', 'close'])
    elif 'price' in df.columns:
        display_cols.extend(['price', 'size'])

    if not display_cols:
        display_cols = list(df.columns[:5])

    # Show first 3 rows
    log_console(f"\nFirst 3 rows:")
    display(df[display_cols].head(3))

    # Show last 3 rows
    if len(df) > 3:
        log_console(f"\nLast 3 rows:")
        display(df[display_cols].tail(3))


async def main():
    """Main test function."""
    log_console("=" * 80)
    log_console("COMPREHENSIVE GET_* PARAMETERS TEST WITH DISPLAY OUTPUT")
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
        log_console(f"OK: Found {len(available)} data series\n")

        # Show available data
        log_console("Available data series:")
        display(available[['asset', 'symbol', 'interval', 'sink', 'first_datetime', 'last_datetime']])

        # Process each data series
        for idx, row in available.iterrows():
            asset = row['asset']
            symbol = row['symbol']
            interval = row['interval']
            sink = row['sink']
            start = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

            log_console("\n" + "=" * 80)
            log_console(f"SERIES: {symbol} ({asset}) - {interval} - {sink}")
            log_console("=" * 80)

            # =====================================================================
            # TEST 1: get_first_n_rows=5
            # =====================================================================
            df, warn = manager.query_local_data(
                asset=asset, symbol=symbol, interval=interval, sink=sink,
                start_date=start, get_first_n_rows=5
            )
            display_extraction(
                f"[1] get_first_n_rows=5 | {symbol} {interval} ({sink})",
                df, warn
            )

            # =====================================================================
            # TEST 2: get_last_n_rows=5
            # =====================================================================
            df, warn = manager.query_local_data(
                asset=asset, symbol=symbol, interval=interval, sink=sink,
                start_date=start, get_last_n_rows=5
            )
            display_extraction(
                f"[2] get_last_n_rows=5 | {symbol} {interval} ({sink})",
                df, warn
            )

            # =====================================================================
            # TEST 3: get_first_n_minutes=5 (only for intraday data)
            # =====================================================================
            if interval in ['tick', '1s', '5s', '10s', '15s', '30s', '1m', '5m', '10m', '15m', '30m', '1h']:
                df, warn = manager.query_local_data(
                    asset=asset, symbol=symbol, interval=interval, sink=sink,
                    start_date=start, get_first_n_minutes=5
                )
                display_extraction(
                    f"[3] get_first_n_minutes=5 | {symbol} {interval} ({sink})",
                    df, warn
                )

            # =====================================================================
            # TEST 4: get_last_n_minutes=5 (only for intraday data)
            # =====================================================================
            if interval in ['tick', '1s', '5s', '10s', '15s', '30s', '1m', '5m', '10m', '15m', '30m', '1h']:
                df, warn = manager.query_local_data(
                    asset=asset, symbol=symbol, interval=interval, sink=sink,
                    get_last_n_minutes=5  # no start_date for last
                )
                display_extraction(
                    f"[4] get_last_n_minutes=5 | {symbol} {interval} ({sink})",
                    df, warn
                )

            # =====================================================================
            # TEST 5: get_first_n_days=2
            # =====================================================================
            df, warn = manager.query_local_data(
                asset=asset, symbol=symbol, interval=interval, sink=sink,
                start_date=start, get_first_n_days=2
            )
            display_extraction(
                f"[5] get_first_n_days=2 | {symbol} {interval} ({sink})",
                df, warn
            )

            # =====================================================================
            # TEST 6: get_last_n_days=2
            # =====================================================================
            df, warn = manager.query_local_data(
                asset=asset, symbol=symbol, interval=interval, sink=sink,
                get_last_n_days=2  # no start_date for last
            )
            display_extraction(
                f"[6] get_last_n_days=2 | {symbol} {interval} ({sink})",
                df, warn
            )

        # =====================================================================
        # SUMMARY
        # =====================================================================
        log_console("\n" + "=" * 80)
        log_console("ALL EXTRACTION TESTS COMPLETED")
        log_console("=" * 80)
        log_console(f"Total data series tested: {len(available)}")
        log_console(f"Tests per series: 6 (rows, minutes, days x 2)")
        log_console(f"Total extractions performed: {len(available) * 6}")


if __name__ == "__main__":
    asyncio.run(main())
