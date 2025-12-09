"""
Test to understand what NQ symbol is in ThetaData
Check if it's Nasdaq-100 E-mini futures or an equity
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import asyncio
from tdSynchManager.client import ThetaDataV3Client

async def main():
    print("=" * 80)
    print("Investigating NQ symbol in ThetaData")
    print("=" * 80)

    async with ThetaDataV3Client() as client:
        symbol = "NQ"

        # Test 1: Get available dates
        print("\n" + "=" * 80)
        print("1. Testing stock_list_dates for NQ")
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

        # Test 2: Try to get EOD data from 2018 (when data was available)
        print("\n" + "=" * 80)
        print("2. Testing stock_history_eod for NQ (March 2018 - last available)")
        print("=" * 80)

        try:
            eod, url = await client.stock_history_eod(
                symbol,
                start_date="20180301",
                end_date="20180313",
                format_type="json"
            )
            print(f"[EOD] URL: {url}")
            print(f"[EOD] Response type: {type(eod)}")

            if isinstance(eod, dict):
                # Show price range to identify if it's futures or equity
                if 'open' in eod and 'high' in eod and 'low' in eod and 'close' in eod:
                    opens = eod.get('open', [])
                    highs = eod.get('high', [])
                    lows = eod.get('low', [])
                    closes = eod.get('close', [])
                    volumes = eod.get('volume', [])

                    print(f"\n[EOD] Price Analysis:")
                    print(f"  Open range: {min(opens) if opens else 'N/A'} - {max(opens) if opens else 'N/A'}")
                    print(f"  High range: {min(highs) if highs else 'N/A'} - {max(highs) if highs else 'N/A'}")
                    print(f"  Low range: {min(lows) if lows else 'N/A'} - {max(lows) if lows else 'N/A'}")
                    print(f"  Close range: {min(closes) if closes else 'N/A'} - {max(closes) if closes else 'N/A'}")
                    print(f"  Volume range: {min(volumes) if volumes else 'N/A'} - {max(volumes) if volumes else 'N/A'}")

                    print(f"\n[EOD] Full response:")
                    print(f"  {eod}")

                    # Analysis
                    print(f"\n[ANALYSIS]:")
                    if closes and max(closes) > 1000:
                        print(f"  Price level: {max(closes):.2f} - Could be Nasdaq-100 futures (typically 15000-21000)")
                    elif closes and max(closes) < 500:
                        print(f"  Price level: {max(closes):.2f} - Likely an equity/ETF (too low for NQ futures)")
                else:
                    print(f"[EOD] Response: {eod}")
            else:
                print(f"[EOD] Response: {eod}")
        except Exception as e:
            print(f"[EOD] ERROR: {e}")

    print("\n" + "=" * 80)
    print("TEST COMPLETED")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
