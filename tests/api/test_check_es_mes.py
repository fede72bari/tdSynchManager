"""
Check if ES and MES are available in ThetaData API
for both stock and option symbol lists
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import asyncio
from tdSynchManager.client import ThetaDataV3Client

async def main():
    print("=" * 80)
    print("Checking ThetaData API for ES and MES symbols")
    print("=" * 80)

    async with ThetaDataV3Client() as client:
        # Check stock symbols
        print("\n" + "=" * 80)
        print("1. Checking STOCK symbols list")
        print("=" * 80)

        try:
            stock_symbols_response, url = await client.stock_list_symbols(format_type="json")
            print(f"[STOCK] URL: {url}")

            # Handle response - it's a dict with 'symbol' key containing the list
            if isinstance(stock_symbols_response, dict) and 'symbol' in stock_symbols_response:
                stock_symbols = stock_symbols_response['symbol']
                print(f"[STOCK] Total symbols returned: {len(stock_symbols)}")

                es_found = []
                mes_found = []

                print(f"[STOCK] Searching through {len(stock_symbols)} symbols...")

                for symbol in stock_symbols:
                    symbol_str = str(symbol).upper()

                    if symbol_str == 'ES' or symbol_str.endswith('/ES'):
                        es_found.append(symbol)
                    if symbol_str == 'MES' or symbol_str.endswith('/MES'):
                        mes_found.append(symbol)

                # Also check for partial matches
                print(f"[STOCK] Searching for symbols containing 'ES' or 'MES'...")
                es_matches = [s for s in stock_symbols if 'ES' in str(s).upper()][:10]
                mes_matches = [s for s in stock_symbols if 'MES' in str(s).upper()][:10]

                if es_found:
                    print(f"\n[STOCK] FOUND: ES matches ({len(es_found)}): {es_found}")
                else:
                    print(f"\n[STOCK] NOT FOUND: ES not in stock symbols")

                if mes_found:
                    print(f"[STOCK] FOUND: MES matches ({len(mes_found)}): {mes_found}")
                else:
                    print(f"[STOCK] NOT FOUND: MES not in stock symbols")

                if es_matches:
                    print(f"\n[STOCK] Symbols containing 'ES' (first 10): {es_matches}")
                if mes_matches:
                    print(f"[STOCK] Symbols containing 'MES' (first 10): {mes_matches}")
            else:
                print(f"[STOCK] Unexpected response format: {type(stock_symbols_response)}")

        except Exception as e:
            print(f"[STOCK] ERROR: {e}")
            import traceback
            traceback.print_exc()

        # Check option symbols
        print("\n" + "=" * 80)
        print("2. Checking OPTION symbols list (underlying)")
        print("=" * 80)

        try:
            option_symbols_response, url = await client.option_list_symbols(format_type="json")
            print(f"[OPTION] URL: {url}")

            # Handle response - it's a dict with 'symbol' key containing the list
            if isinstance(option_symbols_response, dict) and 'symbol' in option_symbols_response:
                option_symbols = option_symbols_response['symbol']
                print(f"[OPTION] Total symbols returned: {len(option_symbols)}")

                es_found = []
                mes_found = []

                print(f"[OPTION] Searching through {len(option_symbols)} symbols...")

                for symbol in option_symbols:
                    symbol_str = str(symbol).upper()

                    if symbol_str == 'ES' or symbol_str.endswith('/ES'):
                        es_found.append(symbol)
                    if symbol_str == 'MES' or symbol_str.endswith('/MES'):
                        mes_found.append(symbol)

                # Also check for partial matches
                print(f"[OPTION] Searching for symbols containing 'ES' or 'MES'...")
                es_matches = [s for s in option_symbols if 'ES' in str(s).upper()][:10]
                mes_matches = [s for s in option_symbols if 'MES' in str(s).upper()][:10]

                if es_found:
                    print(f"\n[OPTION] FOUND: ES matches ({len(es_found)}): {es_found}")
                else:
                    print(f"\n[OPTION] NOT FOUND: ES not in option symbols")

                if mes_found:
                    print(f"[OPTION] FOUND: MES matches ({len(mes_found)}): {mes_found}")
                else:
                    print(f"[OPTION] NOT FOUND: MES not in option symbols")

                if es_matches:
                    print(f"\n[OPTION] Symbols containing 'ES' (first 10): {es_matches}")
                if mes_matches:
                    print(f"[OPTION] Symbols containing 'MES' (first 10): {mes_matches}")
            else:
                print(f"[OPTION] Unexpected response format: {type(option_symbols_response)}")

        except Exception as e:
            print(f"[OPTION] ERROR: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 80)
    print("TEST COMPLETED")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
