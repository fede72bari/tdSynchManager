#!/usr/bin/env python3
"""Test ordering logic for options vs non-options."""
from console_log import log_console

import sys
sys.path.insert(0, 'src')

import pandas as pd
import asyncio
from tdSynchManager.config import ManagerConfig
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client


async def main():
    cfg = ManagerConfig(
        root_dir="./data",
        max_concurrency=5,
        influx_url="http://127.0.0.1:8181",
        influx_bucket="ThetaData",
        influx_token="apiv3_WUNxFGW5CsII-ZTTME1Q4Bycq4DgsUksWwgEuSPZlb1WXdWT5TDyxvHEosashE7Um_bvWSkxaqNmq2ejGGDoZQ"
    )

    async with ThetaDataV3Client(base_url="http://localhost:25503/v3") as client:
        manager = ThetaSyncManager(cfg, client)

        log_console("=" * 80)
        log_console("ORDERING VERIFICATION TESTS")
        log_console("=" * 80)

        available = manager.list_available_data()
        log_console(f"\nAvailable data: {len(available)} series")
        log_console(available[['asset', 'symbol', 'interval', 'sink']].to_string())

        # TEST: Options from InfluxDB (should have exp, strike, timestamp)
        log_console("\n" + "=" * 80)
        log_console("TEST: Options Ordering (InfluxDB)")
        log_console("=" * 80)

        opt_influx = available[(available['asset'] == 'option') & (available['sink'] == 'influxdb')]
        if not opt_influx.empty:
            row = opt_influx.iloc[0]
            start = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

            log_console(f"Querying: {row['symbol']} {row['interval']} from {row['sink']}")

            df, warn = manager.query_local_data(
                asset='option',
                symbol=row['symbol'],
                interval=row['interval'],
                sink='influxdb',
                start_date=start,
                max_rows=20
            )

            if df is not None and len(df) > 0:
                log_console(f"\nGot {len(df)} rows")
                log_console(f"Columns: {list(df.columns)[:10]}...")

                # Check if has expiration, strike
                has_exp = 'expiration' in df.columns
                has_strike = 'strike' in df.columns
                has_ts = 'timestamp' in df.columns

                log_console(f"Has expiration: {has_exp}")
                log_console(f"Has strike: {has_strike}")
                log_console(f"Has timestamp: {has_ts}")

                if has_exp and has_strike and has_ts:
                    # Convert timestamp to datetime for comparison
                    df['timestamp'] = pd.to_datetime(df['timestamp'])

                    # Show sample
                    log_console("\nFirst 10 rows (exp, strike, timestamp):")
                    log_console(df[['expiration', 'strike', 'timestamp']].head(10).to_string())

                    # Check ordering
                    is_sorted = True
                    for i in range(len(df) - 1):
                        exp1 = pd.to_datetime(df.iloc[i]['expiration'])
                        exp2 = pd.to_datetime(df.iloc[i+1]['expiration'])
                        strike1 = df.iloc[i]['strike']
                        strike2 = df.iloc[i+1]['strike']
                        ts1 = df.iloc[i]['timestamp']
                        ts2 = df.iloc[i+1]['timestamp']

                        # Should be: exp ASC, strike ASC, timestamp DESC
                        if exp1 > exp2:
                            log_console(f"\nRow {i}: exp1={exp1} > exp2={exp2} - FAIL (should be ASC)")
                            is_sorted = False
                            break
                        elif exp1 == exp2:
                            if strike1 > strike2:
                                log_console(f"\nRow {i}: strike1={strike1} > strike2={strike2} - FAIL (should be ASC)")
                                is_sorted = False
                                break
                            elif strike1 == strike2:
                                if ts1 < ts2:
                                    log_console(f"\nRow {i}: ts1={ts1} < ts2={ts2} - FAIL (should be DESC)")
                                    is_sorted = False
                                    break

                    if is_sorted:
                        log_console("\nORDERING CHECK: PASS (exp ASC, strike ASC, timestamp DESC)")
                    else:
                        log_console("\nORDERING CHECK: FAIL")
                else:
                    log_console("\nMissing required columns for ordering check")
            else:
                log_console("No data returned")
        else:
            log_console("No option data in InfluxDB")

        # TEST 2: All InfluxDB data with different intervals
        log_console("\n" + "=" * 80)
        log_console("TEST: All InfluxDB series ordering")
        log_console("=" * 80)

        for idx, row in available[available['sink'] == 'influxdb'].iterrows():
            log_console(f"\n{row['symbol']} {row['interval']} ({row['asset']}):")
            start = pd.to_datetime(row['first_datetime']).strftime("%Y-%m-%d")

            df, warn = manager.query_local_data(
                asset=row['asset'],
                symbol=row['symbol'],
                interval=row['interval'],
                sink='influxdb',
                start_date=start,
                max_rows=5
            )

            if df is not None and 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                log_console(f"  Rows: {len(df)}")
                log_console(f"  Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")

                if row['asset'] == 'option' and 'expiration' in df.columns and 'strike' in df.columns:
                    log_console(f"  First row: exp={df.iloc[0]['expiration']}, strike={df.iloc[0]['strike']}, ts={df.iloc[0]['timestamp']}")
                else:
                    log_console(f"  First timestamp: {df.iloc[0]['timestamp']}")
                    log_console(f"  Last timestamp: {df.iloc[-1]['timestamp']}")


if __name__ == "__main__":
    asyncio.run(main())
