#!/usr/bin/env python3
"""Test all sinks (CSV, Parquet, InfluxDB) with get_* parameters."""

import sys
sys.path.insert(0, 'src')

import pandas as pd
import asyncio
from tdSynchManager.config import ManagerConfig
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.client import ThetaDataV3Client


async def main():
    cfg = ManagerConfig(
        root_dir=r'C:\\Users\\Federico\\Downloads',
        max_concurrency=5,
        influx_url='http://127.0.0.1:8181',
        influx_bucket='ThetaData',
        influx_token='apiv3_WUNxFGW5CsII-ZTTME1Q4Bycq4DgsUksWwgEuSPZlb1WXdWT5TDyxvHEosashE7Um_bvWSkxaqNmq2ejGGDoZQ'
    )

    async with ThetaDataV3Client(base_url='http://localhost:25503/v3') as client:
        manager = ThetaSyncManager(cfg, client)

        print("=" * 80)
        print("COMPLETE TEST: ALL SINKS + ALL GET_* PARAMETERS")
        print("=" * 80)

        available = manager.list_available_data()
        print(f"\nTotal available: {len(available)} series")
        print(f"  CSV: {len(available[available['sink'] == 'csv'])}")
        print(f"  Parquet: {len(available[available['sink'] == 'parquet'])}")
        print(f"  InfluxDB: {len(available[available['sink'] == 'influxdb'])}")

        results = []

        # Test each sink with different get_* parameters
        for sink in ['csv', 'parquet', 'influxdb']:
            sink_data = available[available['sink'] == sink]
            if sink_data.empty:
                continue

            row = sink_data.iloc[0]
            start = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

            print(f"\n{'='*80}")
            print(f"TESTING SINK: {sink.upper()}")
            print(f"  Symbol: {row['symbol']}, Interval: {row['interval']}")
            print(f"{'='*80}")

            # TEST 1: get_first_n_rows
            print(f"\n  [1] get_first_n_rows=5")
            df, warn = manager.query_local_data(
                asset=row['asset'], symbol=row['symbol'], interval=row['interval'],
                sink=sink, start_date=start, get_first_n_rows=5
            )
            if df is not None:
                status = "PASS" if len(df) == 5 else f"FAIL (got {len(df)})"
                print(f"      Result: {status}")
                results.append({'sink': sink, 'test': 'get_first_n_rows', 'status': status, 'rows': len(df)})
            else:
                print(f"      Result: FAIL (no data)")
                results.append({'sink': sink, 'test': 'get_first_n_rows', 'status': 'FAIL', 'rows': 0})

            # TEST 2: get_last_n_rows
            print(f"  [2] get_last_n_rows=3")
            df, warn = manager.query_local_data(
                asset=row['asset'], symbol=row['symbol'], interval=row['interval'],
                sink=sink, start_date=start, get_last_n_rows=3
            )
            if df is not None:
                status = "PASS" if len(df) == 3 else f"FAIL (got {len(df)})"
                print(f"      Result: {status}")
                results.append({'sink': sink, 'test': 'get_last_n_rows', 'status': status, 'rows': len(df)})

            # TEST 3: get_first_n_days
            print(f"  [3] get_first_n_days=1")
            df, warn = manager.query_local_data(
                asset=row['asset'], symbol=row['symbol'], interval=row['interval'],
                sink=sink, start_date=start, get_first_n_days=1
            )
            if df is not None and 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                days = (df['timestamp'].max() - df['timestamp'].min()).days
                status = "PASS" if days <= 1 else f"FAIL (got {days} days)"
                print(f"      Result: {status} ({len(df)} rows, {days} days)")
                results.append({'sink': sink, 'test': 'get_first_n_days', 'status': status, 'rows': len(df)})

            # TEST 4: get_last_n_days (no start_date)
            print(f"  [4] get_last_n_days=1 (no start_date)")
            df, warn = manager.query_local_data(
                asset=row['asset'], symbol=row['symbol'], interval=row['interval'],
                sink=sink, get_last_n_days=1
            )
            if df is not None and 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                days = (df['timestamp'].max() - df['timestamp'].min()).days
                status = "PASS" if days <= 1 else f"FAIL (got {days} days)"
                print(f"      Result: {status} ({len(df)} rows, {days} days)")
                results.append({'sink': sink, 'test': 'get_last_n_days', 'status': status, 'rows': len(df)})

            # TEST 5: Ordering check (options)
            print(f"  [5] Ordering check (exp, strike, ts DESC)")
            df, warn = manager.query_local_data(
                asset=row['asset'], symbol=row['symbol'], interval=row['interval'],
                sink=sink, start_date=start, max_rows=20
            )
            if df is not None and 'expiration' in df.columns and 'strike' in df.columns and 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df['expiration'] = pd.to_datetime(df['expiration'])

                # Check ordering
                ordered = True
                for i in range(len(df) - 1):
                    exp1, exp2 = df.iloc[i]['expiration'], df.iloc[i+1]['expiration']
                    s1, s2 = df.iloc[i]['strike'], df.iloc[i+1]['strike']

                    if exp1 > exp2:
                        ordered = False
                        break
                    elif exp1 == exp2 and s1 > s2:
                        ordered = False
                        break

                status = "PASS" if ordered else "FAIL"
                print(f"      Result: {status}")
                results.append({'sink': sink, 'test': 'ordering', 'status': status, 'rows': len(df)})

        # SUMMARY
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)

        df_results = pd.DataFrame(results)
        print(df_results.to_string(index=False))

        total = len(results)
        passed = len([r for r in results if 'PASS' in str(r['status'])])
        print(f"\nTotal tests: {total}")
        print(f"Passed: {passed}")
        print(f"Success rate: {100 * passed / total:.1f}%")


if __name__ == "__main__":
    asyncio.run(main())
