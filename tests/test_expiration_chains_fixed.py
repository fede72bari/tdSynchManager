# ================================================================================
# EXPIRATION CHAINS & STRIKES TESTS - FIXED VERSION
# Tests available_expiration_chains() and available_strikes_by_expiration()
# Handles days without data (weekends, holidays)
# ================================================================================

import sys
sys.path.insert(0, 'src')

import pandas as pd
import asyncio
from tdSynchManager.config import ManagerConfig
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client


def get_available_trading_days(manager, asset, symbol, interval, sink):
    """
    Get list of days that actually have trading data.
    Returns sorted list of date objects.
    """
    # Query all data to see which days have data
    df, _ = manager.query_local_data(
        asset=asset,
        symbol=symbol,
        interval=interval,
        sink=sink,
        get_last_n_rows=50000,  # Get a large sample
        _allow_full_scan=True
    )

    if df is None or len(df) == 0:
        return []

    # Find time column
    time_col = None
    for col in ['timestamp', 'ms_of_day', 'date']:
        if col in df.columns:
            time_col = col
            break

    if not time_col:
        return []

    # Convert to datetime and extract unique dates
    df_copy = df.copy()
    df_copy[time_col] = pd.to_datetime(df_copy[time_col])
    unique_dates = sorted(df_copy[time_col].dt.date.unique())

    return unique_dates


# ================================================================================
# CONFIGURAZIONE
# ================================================================================
cfg = ManagerConfig(
    root_dir=r'tests/data',
    max_concurrency=5,
    influx_url='http://127.0.0.1:8181',
    influx_bucket='ThetaData',
    influx_token='apiv3_WUNxFGW5CsII-ZTTME1Q4Bycq4DgsUksWwgEuSPZlb1WXdWT5TDyxvHEosashE7Um_bvWSkxaqNmq2ejGGDoZQ'
)


async def run_tests():
    """Run all expiration chains and strikes tests."""
    async with ThetaDataV3Client(base_url='http://localhost:25503/v3') as client:
        manager = ThetaSyncManager(cfg, client)

        print("="*80)
        print("TEST: EXPIRATION CHAINS & STRIKES - ALL SINKS & INTERVALS")
        print("Fixed version: Uses actual trading days for date range tests")
        print("="*80)

        # Get available data
        print("\n📊 Scansione dati disponibili...")
        available = manager.list_available_data()

        # Filter only options data
        options_data = available[available['asset'] == 'option']
        print(f"✅ Trovate {len(options_data)} serie di dati per opzioni\n")

        all_results = []

        # Test each series
        for idx, row in options_data.iterrows():
            asset = row['asset']
            symbol = row['symbol']
            interval = row['interval']
            sink = row['sink']

            print(f"\n{'='*80}")
            print(f"TESTING: {symbol} - {interval} - sink={sink}")
            print(f"{'='*80}")

            # ================================================================
            # Get actual trading days with data for range tests
            # ================================================================
            trading_days = get_available_trading_days(manager, asset, symbol, interval, sink)

            if len(trading_days) < 2:
                print(f"⚠️  Meno di 2 giorni con dati disponibili, salto range test")
                # We can still test with existing_chain=True/False, just not date ranges
                use_date_range = False
            else:
                use_date_range = True
                # Use dates that actually have data
                # Pick dates that are NOT first or last
                if len(trading_days) >= 4:
                    # Skip first day, use second day as start
                    # Skip last 2 days, use third-to-last as end
                    range_start = trading_days[1]
                    range_end = trading_days[-3]
                else:
                    # Use what we have
                    range_start = trading_days[0]
                    range_end = trading_days[-1]

                range_start_str = range_start.strftime("%Y-%m-%d")
                range_end_str = range_end.strftime("%Y-%m-%d")

            # ====================================================================
            # TEST 1: Expiration Chains (existing_chain=True)
            # ====================================================================
            print(f"\n{'─'*80}")
            print(f"TEST 1: Expiration Chains (existing_chain=True)")
            print(f"{'─'*80}")

            df, warn = manager.available_expiration_chains(
                symbol=symbol,
                interval=interval,
                sink=sink,
                existing_chain=True
            )

            if df is None or len(df) == 0:
                print(f"❌ FAIL: Nessuna expiration trovata (potrebbe essere normale se nessuna chain è attiva)")
                all_results.append((f'{symbol}-{interval}-{sink}', 'exp_chains_active', 'WARN', 0))
            else:
                print(f"✅ OK: Trovate {len(df)} catene di expiration attive")
                print(f"\nPrime 5 expiration:")
                print(df.head().to_string(index=False))
                all_results.append((f'{symbol}-{interval}-{sink}', 'exp_chains_active', 'PASS', len(df)))

            # ====================================================================
            # TEST 2: Expiration Chains (existing_chain=False - tutte storiche)
            # ====================================================================
            print(f"\n{'─'*80}")
            print(f"TEST 2: Expiration Chains (existing_chain=False - tutte storiche)")
            print(f"{'─'*80}")

            df, warn = manager.available_expiration_chains(
                symbol=symbol,
                interval=interval,
                sink=sink,
                existing_chain=False
            )

            if df is None or len(df) == 0:
                print(f"❌ FAIL: Nessuna expiration trovata (inatteso!)")
                all_results.append((f'{symbol}-{interval}-{sink}', 'exp_chains_all', 'FAIL', 0))
            else:
                print(f"✅ OK: Trovate {len(df)} catene di expiration totali (storiche + attive)")
                print(f"\nPrime 5 expiration:")
                print(df.head().to_string(index=False))
                if len(df) > 5:
                    print(f"\nUltime 5 expiration:")
                    print(df.tail().to_string(index=False))
                all_results.append((f'{symbol}-{interval}-{sink}', 'exp_chains_all', 'PASS', len(df)))

            # ====================================================================
            # TEST 3: Expiration Chains (range di date specifico)
            # Only if we have enough trading days
            # ====================================================================
            if use_date_range:
                print(f"\n{'─'*80}")
                print(f"TEST 3: Expiration Chains (range di date specifico)")
                print(f"{'─'*80}")
                print(f"Range testato: {range_start_str} → {range_end_str}")

                df, warn = manager.available_expiration_chains(
                    symbol=symbol,
                    interval=interval,
                    sink=sink,
                    existing_chain=False,
                    active_from_date=range_start_str,
                    active_to_date=range_end_str
                )

                if df is None or len(df) == 0:
                    print(f"⚠️  WARN: Nessuna expiration trovata nel range (potrebbe essere normale)")
                    all_results.append((f'{symbol}-{interval}-{sink}', 'exp_chains_range', 'WARN', 0))
                else:
                    print(f"✅ OK: Trovate {len(df)} expiration nel range")
                    print(df.to_string(index=False))
                    all_results.append((f'{symbol}-{interval}-{sink}', 'exp_chains_range', 'PASS', len(df)))

            # ====================================================================
            # TEST 4: Strikes per Expiration (tutte)
            # ====================================================================
            print(f"\n{'─'*80}")
            print(f"TEST 4: Strikes per Expiration (tutte)")
            print(f"{'─'*80}")

            strikes_dict, warn = manager.available_strikes_by_expiration(
                symbol=symbol,
                interval=interval,
                sink=sink
            )

            if strikes_dict is None or len(strikes_dict) == 0:
                print(f"❌ FAIL: Nessun strike trovato")
                all_results.append((f'{symbol}-{interval}-{sink}', 'strikes_all', 'FAIL', 0))
            else:
                # Count total unique strikes across all expirations
                all_strikes = set()
                for strikes_list in strikes_dict.values():
                    all_strikes.update(strikes_list)

                print(f"✅ OK: Trovati strike per {len(strikes_dict)} expiration (totale {len(all_strikes)} strike unici)")

                # Show first 3 expirations with their strikes
                print(f"\nPrime 3 expiration con i loro strike:")
                for i, (exp_date, strikes) in enumerate(list(strikes_dict.items())[:3]):
                    print(f"  {exp_date}: {strikes[:6] if len(strikes) > 6 else strikes} ({len(strikes)} strike)")

                all_results.append((f'{symbol}-{interval}-{sink}', 'strikes_all', 'PASS', len(all_strikes)))

            # ====================================================================
            # TEST 5: Strikes per Expiration (specifiche)
            # ====================================================================
            print(f"\n{'─'*80}")
            print(f"TEST 5: Strikes per Expiration (specifiche)")
            print(f"{'─'*80}")

            # Get first 2 expirations from previous test
            if strikes_dict and len(strikes_dict) >= 2:
                first_two_expirations = list(strikes_dict.keys())[:2]
                print(f"Expiration selezionate: {first_two_expirations}")

                strikes_dict_filtered, warn = manager.available_strikes_by_expiration(
                    symbol=symbol,
                    interval=interval,
                    sink=sink,
                    expirations=first_two_expirations
                )

                if strikes_dict_filtered is None or len(strikes_dict_filtered) == 0:
                    print(f"❌ FAIL: Nessun strike trovato")
                    all_results.append((f'{symbol}-{interval}-{sink}', 'strikes_specific', 'FAIL', 0))
                else:
                    # Count total unique strikes
                    all_strikes = set()
                    for strikes_list in strikes_dict_filtered.values():
                        all_strikes.update(strikes_list)

                    print(f"✅ OK: Trovati strike per {len(strikes_dict_filtered)} expiration")
                    for exp_date, strikes in strikes_dict_filtered.items():
                        print(f"  {exp_date}: {strikes[:6] if len(strikes) > 6 else strikes} ({len(strikes)} strike)")

                    all_results.append((f'{symbol}-{interval}-{sink}', 'strikes_specific', 'PASS', len(all_strikes)))
            else:
                print(f"⚠️  SKIP: Non abbastanza expiration per questo test")
                all_results.append((f'{symbol}-{interval}-{sink}', 'strikes_specific', 'SKIP', 0))

        # ====================================================================
        # FINAL SUMMARY
        # ====================================================================
        print(f"\n{'='*80}")
        print(f"RIEPILOGO FINALE - TUTTI I TEST")
        print(f"{'='*80}\n")

        # Create summary DataFrame
        df_summary = pd.DataFrame(all_results, columns=['Series', 'Test', 'Status', 'Count'])
        print(df_summary.to_string(index=False))

        # Count results
        total = len(all_results)
        passed = len([r for r in all_results if r[2] == 'PASS'])
        failed = len([r for r in all_results if r[2] == 'FAIL'])
        warned = len([r for r in all_results if r[2] == 'WARN'])
        skipped = len([r for r in all_results if r[2] == 'SKIP'])

        print(f"\n{'='*80}")
        print(f"Total tests: {total}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"⚠️  Warnings: {warned}")
        print(f"⏭️  Skipped: {skipped}")

        if total > 0:
            # Calculate success rate (PASS + WARN are considered acceptable)
            success_rate = 100 * (passed + warned) / total
            print(f"Success rate: {success_rate:.1f}%")

        print(f"{'='*80}")


# Run the tests
await run_tests()
