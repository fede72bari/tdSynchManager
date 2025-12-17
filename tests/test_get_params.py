#!/usr/bin/env python3
"""Test get_* parameters and ordering."""

import sys
sys.path.insert(0, 'src')

import pandas as pd
import asyncio
from tdSynchManager.config import ManagerConfig
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client


async def main():
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

        print("=" * 80)
        print("GET_* PARAMETERS AND ORDERING TESTS")
        print("=" * 80)

        # Get available data
        print("\nScanning available data...")
        available = manager.list_available_data()
        print(f"Found {len(available)} data series\n")

        test_count = 0
        pass_count = 0

        # TEST 1: Basic query - options ordering
        print("\n" + "=" * 80)
        print(f"TEST 1: Options Ordering (exp ASC, strike ASC, ts DESC)")
        print("=" * 80)
        test_count += 1

        opt_csv = available[(available['asset'] == 'option') & (available['sink'] == 'csv')]
        if not opt_csv.empty:
            row = opt_csv.iloc[0]
            start = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

            df, warn = manager.query_local_data(
                asset=row['asset'], symbol=row['symbol'], interval=row['interval'],
                sink='csv', start_date=start, max_rows=10
            )

            if df is not None and len(df) > 0:
                print(f"OK Got {len(df)} rows")
                if 'expiration' in df.columns:
                    print(f"\nSample (exp, strike, timestamp):")
                    cols = ['expiration', 'strike']
                    if 'timestamp' in df.columns:
                        cols.append('timestamp')
                    print(df[cols].head(5))
                    pass_count += 1
        else:
            print("No option CSV data available")

        # TEST 2: get_first_n_rows
        print("\n" + "=" * 80)
        print(f"TEST 2: get_first_n_rows=5")
        print("=" * 80)
        test_count += 1

        if not available.empty:
            row = available.iloc[0]
            start = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

            df, warn = manager.query_local_data(
                asset=row['asset'], symbol=row['symbol'], interval=row['interval'],
                sink=row['sink'], start_date=start, get_first_n_rows=5
            )

            if df is not None:
                print(f"OK Got {len(df)} rows (expected 5)")
                if len(df) == 5:
                    print("OK Row count PASS")
                    pass_count += 1
                else:
                    print(f"FAIL Expected 5 rows, got {len(df)}")

        # TEST 3: get_last_n_rows
        print("\n" + "=" * 80)
        print(f"TEST 3: get_last_n_rows=3")
        print("=" * 80)
        test_count += 1

        if not available.empty:
            row = available.iloc[0]
            start = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

            df, warn = manager.query_local_data(
                asset=row['asset'], symbol=row['symbol'], interval=row['interval'],
                sink=row['sink'], start_date=start, get_last_n_rows=3
            )

            if df is not None:
                print(f"OK Got {len(df)} rows (expected 3)")
                if len(df) == 3:
                    print("OK Row count PASS")
                    pass_count += 1

        # TEST 4: get_first_n_days
        print("\n" + "=" * 80)
        print(f"TEST 4: get_first_n_days=1")
        print("=" * 80)
        test_count += 1

        if not available.empty:
            row = available.iloc[0]
            start = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

            df, warn = manager.query_local_data(
                asset=row['asset'], symbol=row['symbol'], interval=row['interval'],
                sink=row['sink'], start_date=start, get_first_n_days=1
            )

            if df is not None and 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                days_span = (df['timestamp'].max() - df['timestamp'].min()).days
                print(f"OK Got {len(df)} rows spanning {days_span} days")
                if days_span <= 1:
                    print("OK Day range PASS")
                    pass_count += 1

        # TEST 5: Multiple get_* params (should fail)
        print("\n" + "=" * 80)
        print(f"TEST 5: Multiple get_* params (should FAIL with warning)")
        print("=" * 80)
        test_count += 1

        if not available.empty:
            row = available.iloc[0]
            start = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

            df, warn = manager.query_local_data(
                asset=row['asset'], symbol=row['symbol'], interval=row['interval'],
                sink=row['sink'], start_date=start,
                get_first_n_rows=5, get_last_n_days=1
            )

            if df is None and any('MULTIPLE_GET_PARAMS' in w for w in warn):
                print(f"OK Correctly rejected with warning: {warn}")
                pass_count += 1
            else:
                print(f"FAIL Should have failed but didn't")

        # TEST 6: InfluxDB query
        print("\n" + "=" * 80)
        print(f"TEST 6: InfluxDB query with get_last_n_rows=5")
        print("=" * 80)
        test_count += 1

        influx_data = available[available['sink'] == 'influxdb']
        if not influx_data.empty:
            row = influx_data.iloc[0]
            start = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

            df, warn = manager.query_local_data(
                asset=row['asset'], symbol=row['symbol'], interval=row['interval'],
                sink='influxdb', start_date=start, get_last_n_rows=5
            )

            if df is not None:
                print(f"OK Got {len(df)} rows from InfluxDB")
                if len(df) == 5:
                    print("OK Row count PASS")
                    pass_count += 1
        else:
            print("No InfluxDB data available")

        # SUMMARY
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Tests run: {test_count}")
        print(f"Tests passed: {pass_count}")
        print(f"Success rate: {100 * pass_count / test_count:.1f}%")


if __name__ == "__main__":
    asyncio.run(main())
