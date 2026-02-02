"""
Test to understand what NQ symbol is in ThetaData
Check if it's Nasdaq-100 E-mini futures or an equity
"""
from console_log import log_console

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import asyncio
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client

async def main():
    log_console("=" * 80)
    log_console("Investigating NQ symbol in ThetaData")
    log_console("=" * 80)

    async with ThetaDataV3Client() as client:
        symbol = "NQ"

        # Test 1: Get available dates
        log_console("\n" + "=" * 80)
        log_console("1. Testing stock_list_dates for NQ")
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

        # Test 2: Try to get EOD data from 2018 (when data was available)
        log_console("\n" + "=" * 80)
        log_console("2. Testing stock_history_eod for NQ (March 2018 - last available)")
        log_console("=" * 80)

        try:
            eod, url = await client.stock_history_eod(
                symbol,
                start_date="20180301",
                end_date="20180313",
                format_type="json"
            )
            log_console(f"[EOD] URL: {url}")
            log_console(f"[EOD] Response type: {type(eod)}")

            if isinstance(eod, dict):
                # Show price range to identify if it's futures or equity
                if 'open' in eod and 'high' in eod and 'low' in eod and 'close' in eod:
                    opens = eod.get('open', [])
                    highs = eod.get('high', [])
                    lows = eod.get('low', [])
                    closes = eod.get('close', [])
                    volumes = eod.get('volume', [])

                    log_console(f"\n[EOD] Price Analysis:")
                    log_console(f"  Open range: {min(opens) if opens else 'N/A'} - {max(opens) if opens else 'N/A'}")
                    log_console(f"  High range: {min(highs) if highs else 'N/A'} - {max(highs) if highs else 'N/A'}")
                    log_console(f"  Low range: {min(lows) if lows else 'N/A'} - {max(lows) if lows else 'N/A'}")
                    log_console(f"  Close range: {min(closes) if closes else 'N/A'} - {max(closes) if closes else 'N/A'}")
                    log_console(f"  Volume range: {min(volumes) if volumes else 'N/A'} - {max(volumes) if volumes else 'N/A'}")

                    log_console(f"\n[EOD] Full response:")
                    log_console(f"  {eod}")

                    # Analysis
                    log_console(f"\n[ANALYSIS]:")
                    if closes and max(closes) > 1000:
                        log_console(f"  Price level: {max(closes):.2f} - Could be Nasdaq-100 futures (typically 15000-21000)")
                    elif closes and max(closes) < 500:
                        log_console(f"  Price level: {max(closes):.2f} - Likely an equity/ETF (too low for NQ futures)")
                else:
                    log_console(f"[EOD] Response: {eod}")
            else:
                log_console(f"[EOD] Response: {eod}")
        except Exception as e:
            log_console(f"[EOD] ERROR: {e}")

    log_console("\n" + "=" * 80)
    log_console("TEST COMPLETED")
    log_console("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
