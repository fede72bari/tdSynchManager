"""
Test to understand what ES symbol is in ThetaData
Check snapshot and available dates
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import asyncio
from tdSynchManager.client import ThetaDataV3Client

async def main():
    print("=" * 80)
    print("Investigating ES symbol in ThetaData")
    print("=" * 80)

    async with ThetaDataV3Client() as client:
        symbol = "ES"

        # Test 1: Get OHLC snapshot
        print("\n" + "=" * 80)
        print("1. Testing stock_snapshot_ohlc for ES")
        print("=" * 80)

        try:
            snapshot, url = await client.stock_snapshot_ohlc(symbol, format_type="json")
            print(f"[OHLC] URL: {url}")
            print(f"[OHLC] Response type: {type(snapshot)}")
            print(f"[OHLC] Response: {snapshot}")
        except Exception as e:
            print(f"[OHLC] ERROR: {e}")

        # Test 2: Get trade snapshot
        print("\n" + "=" * 80)
        print("2. Testing stock_snapshot_trade for ES")
        print("=" * 80)

        try:
            trade, url = await client.stock_snapshot_trade(symbol, format_type="json")
            print(f"[TRADE] URL: {url}")
            print(f"[TRADE] Response type: {type(trade)}")
            print(f"[TRADE] Response: {trade}")
        except Exception as e:
            print(f"[TRADE] ERROR: {e}")

        # Test 3: Get available dates
        print("\n" + "=" * 80)
        print("3. Testing stock_list_dates for ES")
        print("=" * 80)

        try:
            dates, url = await client.stock_list_dates(symbol, data_type="trade", format_type="json")
            print(f"[DATES] URL: {url}")
            print(f"[DATES] Response type: {type(dates)}")
            if isinstance(dates, dict) and 'date' in dates:
                date_list = dates['date']
                print(f"[DATES] Total dates available: {len(date_list)}")
                print(f"[DATES] First 10 dates: {date_list[:10]}")
                print(f"[DATES] Last 10 dates: {date_list[-10:]}")
            else:
                print(f"[DATES] Response: {dates}")
        except Exception as e:
            print(f"[DATES] ERROR: {e}")

        # Test 4: Try to get EOD data for a recent date
        print("\n" + "=" * 80)
        print("4. Testing stock_history_eod for ES (last 5 days)")
        print("=" * 80)

        try:
            eod, url = await client.stock_history_eod(
                symbol,
                start_date="20241201",
                end_date="20241205",
                format_type="json"
            )
            print(f"[EOD] URL: {url}")
            print(f"[EOD] Response type: {type(eod)}")
            print(f"[EOD] Response: {eod}")
        except Exception as e:
            print(f"[EOD] ERROR: {e}")

    print("\n" + "=" * 80)
    print("TEST COMPLETED")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
