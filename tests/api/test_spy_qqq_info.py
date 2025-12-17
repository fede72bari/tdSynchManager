"""
Test SPY and QQQ symbols in ThetaData
These should be the major ETFs
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import asyncio
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client

async def test_symbol(client, symbol):
    print("\n" + "=" * 80)
    print(f"Testing {symbol}")
    print("=" * 80)

    # Test 1: Get available dates
    print(f"\n[{symbol}] 1. Checking available dates")
    try:
        dates, url = await client.stock_list_dates(symbol, data_type="trade", format_type="json")
        if isinstance(dates, dict) and 'date' in dates:
            date_list = dates['date']
            print(f"[{symbol}] Total dates: {len(date_list)}")
            print(f"[{symbol}] First date: {date_list[0] if date_list else 'N/A'}")
            print(f"[{symbol}] Last date: {date_list[-1] if date_list else 'N/A'}")
        else:
            print(f"[{symbol}] Unexpected response: {dates}")
    except Exception as e:
        print(f"[{symbol}] ERROR on dates: {e}")

    # Test 2: Get recent EOD data
    print(f"\n[{symbol}] 2. Getting recent EOD data (Dec 1-5, 2024)")
    try:
        eod, url = await client.stock_history_eod(
            symbol,
            start_date="20241201",
            end_date="20241205",
            format_type="json"
        )

        if isinstance(eod, dict):
            if 'close' in eod and eod['close']:
                closes = eod.get('close', [])
                volumes = eod.get('volume', [])

                print(f"[{symbol}] Price range: ${min(closes):.2f} - ${max(closes):.2f}")
                print(f"[{symbol}] Last close: ${closes[-1]:.2f}")
                print(f"[{symbol}] Volume range: {min(volumes):,} - {max(volumes):,}")
                print(f"[{symbol}] Avg volume: {sum(volumes)//len(volumes):,}")

                # Analysis
                if symbol == "SPY" and 400 < max(closes) < 700:
                    print(f"[{symbol}] CONFIRMED: This is SPY (SPDR S&P 500 ETF)")
                elif symbol == "QQQ" and 300 < max(closes) < 600:
                    print(f"[{symbol}] CONFIRMED: This is QQQ (Invesco Nasdaq-100 ETF)")
            else:
                print(f"[{symbol}] Response: {eod}")
        else:
            print(f"[{symbol}] Unexpected response type: {type(eod)}")
    except Exception as e:
        print(f"[{symbol}] ERROR on EOD: {e}")

    # Test 3: Check if options are available
    print(f"\n[{symbol}] 3. Checking if options are available")
    try:
        expirations, url = await client.option_list_expirations(symbol, format_type="json")
        if isinstance(expirations, dict) and 'expiration' in expirations:
            exp_list = expirations['expiration']
            print(f"[{symbol}] OPTIONS AVAILABLE! Total expirations: {len(exp_list)}")
            print(f"[{symbol}] First 5 expirations: {exp_list[:5]}")
            print(f"[{symbol}] Last 5 expirations: {exp_list[-5:]}")
        else:
            print(f"[{symbol}] Options response: {expirations}")
    except Exception as e:
        print(f"[{symbol}] ERROR on options: {e}")


async def main():
    print("=" * 80)
    print("Testing SPY and QQQ in ThetaData")
    print("=" * 80)

    async with ThetaDataV3Client() as client:
        await test_symbol(client, "SPY")
        await test_symbol(client, "QQQ")

    print("\n" + "=" * 80)
    print("TEST COMPLETED")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
