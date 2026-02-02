"""
Check if ES and MES are available in ThetaData API
for both stock and option symbol lists
"""
from console_log import log_console

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import asyncio
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client

async def main():
    log_console("=" * 80)
    log_console("Checking ThetaData API for ES and MES symbols")
    log_console("=" * 80)

    async with ThetaDataV3Client() as client:
        # Check stock symbols
        log_console("\n" + "=" * 80)
        log_console("1. Checking STOCK symbols list")
        log_console("=" * 80)

        try:
            stock_symbols_response, url = await client.stock_list_symbols(format_type="json")
            log_console(f"[STOCK] URL: {url}")

            # Handle response - it's a dict with 'symbol' key containing the list
            if isinstance(stock_symbols_response, dict) and 'symbol' in stock_symbols_response:
                stock_symbols = stock_symbols_response['symbol']
                log_console(f"[STOCK] Total symbols returned: {len(stock_symbols)}")

                es_found = []
                mes_found = []

                log_console(f"[STOCK] Searching through {len(stock_symbols)} symbols...")

                for symbol in stock_symbols:
                    symbol_str = str(symbol).upper()

                    if symbol_str == 'ES' or symbol_str.endswith('/ES'):
                        es_found.append(symbol)
                    if symbol_str == 'MES' or symbol_str.endswith('/MES'):
                        mes_found.append(symbol)

                # Also check for partial matches
                log_console(f"[STOCK] Searching for symbols containing 'ES' or 'MES'...")
                es_matches = [s for s in stock_symbols if 'ES' in str(s).upper()][:10]
                mes_matches = [s for s in stock_symbols if 'MES' in str(s).upper()][:10]

                if es_found:
                    log_console(f"\n[STOCK] FOUND: ES matches ({len(es_found)}): {es_found}")
                else:
                    log_console(f"\n[STOCK] NOT FOUND: ES not in stock symbols")

                if mes_found:
                    log_console(f"[STOCK] FOUND: MES matches ({len(mes_found)}): {mes_found}")
                else:
                    log_console(f"[STOCK] NOT FOUND: MES not in stock symbols")

                if es_matches:
                    log_console(f"\n[STOCK] Symbols containing 'ES' (first 10): {es_matches}")
                if mes_matches:
                    log_console(f"[STOCK] Symbols containing 'MES' (first 10): {mes_matches}")
            else:
                log_console(f"[STOCK] Unexpected response format: {type(stock_symbols_response)}")

        except Exception as e:
            log_console(f"[STOCK] ERROR: {e}")
            import traceback
            traceback.print_exc()

        # Check option symbols
        log_console("\n" + "=" * 80)
        log_console("2. Checking OPTION symbols list (underlying)")
        log_console("=" * 80)

        try:
            option_symbols_response, url = await client.option_list_symbols(format_type="json")
            log_console(f"[OPTION] URL: {url}")

            # Handle response - it's a dict with 'symbol' key containing the list
            if isinstance(option_symbols_response, dict) and 'symbol' in option_symbols_response:
                option_symbols = option_symbols_response['symbol']
                log_console(f"[OPTION] Total symbols returned: {len(option_symbols)}")

                es_found = []
                mes_found = []

                log_console(f"[OPTION] Searching through {len(option_symbols)} symbols...")

                for symbol in option_symbols:
                    symbol_str = str(symbol).upper()

                    if symbol_str == 'ES' or symbol_str.endswith('/ES'):
                        es_found.append(symbol)
                    if symbol_str == 'MES' or symbol_str.endswith('/MES'):
                        mes_found.append(symbol)

                # Also check for partial matches
                log_console(f"[OPTION] Searching for symbols containing 'ES' or 'MES'...")
                es_matches = [s for s in option_symbols if 'ES' in str(s).upper()][:10]
                mes_matches = [s for s in option_symbols if 'MES' in str(s).upper()][:10]

                if es_found:
                    log_console(f"\n[OPTION] FOUND: ES matches ({len(es_found)}): {es_found}")
                else:
                    log_console(f"\n[OPTION] NOT FOUND: ES not in option symbols")

                if mes_found:
                    log_console(f"[OPTION] FOUND: MES matches ({len(mes_found)}): {mes_found}")
                else:
                    log_console(f"[OPTION] NOT FOUND: MES not in option symbols")

                if es_matches:
                    log_console(f"\n[OPTION] Symbols containing 'ES' (first 10): {es_matches}")
                if mes_matches:
                    log_console(f"[OPTION] Symbols containing 'MES' (first 10): {mes_matches}")
            else:
                log_console(f"[OPTION] Unexpected response format: {type(option_symbols_response)}")

        except Exception as e:
            log_console(f"[OPTION] ERROR: {e}")
            import traceback
            traceback.print_exc()

    log_console("\n" + "=" * 80)
    log_console("TEST COMPLETED")
    log_console("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
