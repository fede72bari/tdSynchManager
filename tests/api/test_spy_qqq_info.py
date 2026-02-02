"""
Test SPY and QQQ symbols in ThetaData
These should be the major ETFs
"""
from console_log import log_console

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import asyncio
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client

async def test_symbol(client, symbol):
    log_console("\n" + "=" * 80)
    log_console(f"Testing {symbol}")
    log_console("=" * 80)

    # Test 1: Get available dates
    log_console(f"\n[{symbol}] 1. Checking available dates")
    try:
        dates, url = await client.stock_list_dates(symbol, data_type="trade", format_type="json")
        if isinstance(dates, dict) and 'date' in dates:
            date_list = dates['date']
            log_console(f"[{symbol}] Total dates: {len(date_list)}")
            log_console(f"[{symbol}] First date: {date_list[0] if date_list else 'N/A'}")
            log_console(f"[{symbol}] Last date: {date_list[-1] if date_list else 'N/A'}")
        else:
            log_console(f"[{symbol}] Unexpected response: {dates}")
    except Exception as e:
        log_console(f"[{symbol}] ERROR on dates: {e}")

    # Test 2: Get recent EOD data
    log_console(f"\n[{symbol}] 2. Getting recent EOD data (Dec 1-5, 2024)")
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

                log_console(f"[{symbol}] Price range: ${min(closes):.2f} - ${max(closes):.2f}")
                log_console(f"[{symbol}] Last close: ${closes[-1]:.2f}")
                log_console(f"[{symbol}] Volume range: {min(volumes):,} - {max(volumes):,}")
                log_console(f"[{symbol}] Avg volume: {sum(volumes)//len(volumes):,}")

                # Analysis
                if symbol == "SPY" and 400 < max(closes) < 700:
                    log_console(f"[{symbol}] CONFIRMED: This is SPY (SPDR S&P 500 ETF)")
                elif symbol == "QQQ" and 300 < max(closes) < 600:
                    log_console(f"[{symbol}] CONFIRMED: This is QQQ (Invesco Nasdaq-100 ETF)")
            else:
                log_console(f"[{symbol}] Response: {eod}")
        else:
            log_console(f"[{symbol}] Unexpected response type: {type(eod)}")
    except Exception as e:
        log_console(f"[{symbol}] ERROR on EOD: {e}")

    # Test 3: Check if options are available
    log_console(f"\n[{symbol}] 3. Checking if options are available")
    try:
        expirations, url = await client.option_list_expirations(symbol, format_type="json")
        if isinstance(expirations, dict) and 'expiration' in expirations:
            exp_list = expirations['expiration']
            log_console(f"[{symbol}] OPTIONS AVAILABLE! Total expirations: {len(exp_list)}")
            log_console(f"[{symbol}] First 5 expirations: {exp_list[:5]}")
            log_console(f"[{symbol}] Last 5 expirations: {exp_list[-5:]}")
        else:
            log_console(f"[{symbol}] Options response: {expirations}")
    except Exception as e:
        log_console(f"[{symbol}] ERROR on options: {e}")


async def main():
    log_console("=" * 80)
    log_console("Testing SPY and QQQ in ThetaData")
    log_console("=" * 80)

    async with ThetaDataV3Client() as client:
        await test_symbol(client, "SPY")
        await test_symbol(client, "QQQ")

    log_console("\n" + "=" * 80)
    log_console("TEST COMPLETED")
    log_console("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
