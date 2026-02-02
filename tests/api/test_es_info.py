"""
Test to understand what ES symbol is in ThetaData
Check snapshot and available dates
"""
from console_log import log_console

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import asyncio
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client

async def main():
    log_console("=" * 80)
    log_console("Investigating ES symbol in ThetaData")
    log_console("=" * 80)

    async with ThetaDataV3Client() as client:
        symbol = "ES"

        # Test 1: Get OHLC snapshot
        log_console("\n" + "=" * 80)
        log_console("1. Testing stock_snapshot_ohlc for ES")
        log_console("=" * 80)

        try:
            snapshot, url = await client.stock_snapshot_ohlc(symbol, format_type="json")
            log_console(f"[OHLC] URL: {url}")
            log_console(f"[OHLC] Response type: {type(snapshot)}")
            log_console(f"[OHLC] Response: {snapshot}")
        except Exception as e:
            log_console(f"[OHLC] ERROR: {e}")

        # Test 2: Get trade snapshot
        log_console("\n" + "=" * 80)
        log_console("2. Testing stock_snapshot_trade for ES")
        log_console("=" * 80)

        try:
            trade, url = await client.stock_snapshot_trade(symbol, format_type="json")
            log_console(f"[TRADE] URL: {url}")
            log_console(f"[TRADE] Response type: {type(trade)}")
            log_console(f"[TRADE] Response: {trade}")
        except Exception as e:
            log_console(f"[TRADE] ERROR: {e}")

        # Test 3: Get available dates
        log_console("\n" + "=" * 80)
        log_console("3. Testing stock_list_dates for ES")
        log_console("=" * 80)

        try:
            dates, url = await client.stock_list_dates(symbol, data_type="trade", format_type="json")
            log_console(f"[DATES] URL: {url}")
            log_console(f"[DATES] Response type: {type(dates)}")
            if isinstance(dates, dict) and 'date' in dates:
                date_list = dates['date']
                log_console(f"[DATES] Total dates available: {len(date_list)}")
                log_console(f"[DATES] First 10 dates: {date_list[:10]}")
                log_console(f"[DATES] Last 10 dates: {date_list[-10:]}")
            else:
                log_console(f"[DATES] Response: {dates}")
        except Exception as e:
            log_console(f"[DATES] ERROR: {e}")

        # Test 4: Try to get EOD data for a recent date
        log_console("\n" + "=" * 80)
        log_console("4. Testing stock_history_eod for ES (last 5 days)")
        log_console("=" * 80)

        try:
            eod, url = await client.stock_history_eod(
                symbol,
                start_date="20241201",
                end_date="20241205",
                format_type="json"
            )
            log_console(f"[EOD] URL: {url}")
            log_console(f"[EOD] Response type: {type(eod)}")
            log_console(f"[EOD] Response: {eod}")
        except Exception as e:
            log_console(f"[EOD] ERROR: {e}")

    log_console("\n" + "=" * 80)
    log_console("TEST COMPLETED")
    log_console("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
