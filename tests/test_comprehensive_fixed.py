from console_log import log_console
# ================================================================================
# COMPREHENSIVE QUERY TESTS - NOTEBOOK VERSION (FIXED)
# Tests all date combinations, all intervals (tick, minutes, 1d), all sinks
# Handles days without data (weekends, holidays)
# ================================================================================

import sys
sys.path.insert(0, 'src')

import pandas as pd
import asyncio
from tdSynchManager.config import ManagerConfig
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client


def normalize_tz(dt):
    """Remove timezone from datetime for comparison."""
    if dt is None:
        return None
    if hasattr(dt, 'tz') and dt.tz is not None:
        return dt.tz_localize(None)
    return dt


def get_available_trading_days(manager, asset, symbol, interval, sink):
    """
    Get list of days that actually have trading data.
    Returns sorted list of date objects.
    """
    log_console(f"   🔍 Analisi giorni disponibili per {symbol} {interval} ({sink})...")

    # Query all data to see which days have data
    df, _ = manager.query_local_data(
        asset=asset,
        symbol=symbol,
        interval=interval,
        sink=sink,
        get_last_n_rows=50000  # Get a large sample
    )

    if df is None or len(df) == 0:
        log_console(f"   ⚠️  Nessun dato disponibile!")
        return []

    # Find time column
    time_col = None
    for col in ['timestamp', 'ms_of_day', 'date']:
        if col in df.columns:
            time_col = col
            break

    if not time_col:
        log_console(f"   ⚠️  Colonna timestamp non trovata!")
        return []

    # Convert to datetime and extract unique dates
    df_copy = df.copy()
    df_copy[time_col] = pd.to_datetime(df_copy[time_col])
    unique_dates = sorted(df_copy[time_col].dt.date.unique())

    log_console(f"   ✅ Trovati {len(unique_dates)} giorni con dati: {unique_dates[0]} → {unique_dates[-1]}")

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

def validate_result(test_type, df, warnings, start_date_str=None, end_date_str=None,
                   get_first_n_minutes=None, get_last_n_minutes=None,
                   get_first_n_days=None, get_last_n_days=None):
    """Valida la coerenza logica del risultato."""
    issues = []

    if df is None or len(df) == 0:
        return issues

    # Trova colonna timestamp
    time_col = None
    for col in ['timestamp', 'ms_of_day', 'date']:
        if col in df.columns:
            time_col = col
            break

    if not time_col:
        return issues

    df_copy = df.copy()
    df_copy[time_col] = pd.to_datetime(df_copy[time_col])

    time_min = df_copy[time_col].min()
    time_max = df_copy[time_col].max()

    # VALIDAZIONE 1: get_first_n_days dovrebbe restituire dati SOLO nel primo giorno calendario
    if get_first_n_days == 1:
        first_date = time_min.date()
        last_date = time_max.date()
        days_diff = (last_date - first_date).days
        if days_diff > 0:
            issues.append(f"❌ INCOERENTE: get_first_n_days=1 restituisce {days_diff+1} giorni calendario ({first_date} → {last_date})")

    # VALIDAZIONE 2: get_last_n_days dovrebbe restituire dati SOLO nell'ultimo giorno calendario
    if get_last_n_days == 1:
        first_date = time_min.date()
        last_date = time_max.date()
        days_diff = (last_date - first_date).days
        if days_diff > 0:
            issues.append(f"❌ INCOERENTE: get_last_n_days=1 restituisce {days_diff+1} giorni calendario ({first_date} → {last_date})")

    # VALIDAZIONE 3: get_first_n_minutes dovrebbe avere span ≤ n minuti
    if get_first_n_minutes:
        span_minutes = (time_max - time_min).total_seconds() / 60
        if span_minutes > get_first_n_minutes + 0.1:  # tolleranza 6 secondi
            issues.append(f"❌ INCOERENTE: get_first_n_minutes={get_first_n_minutes} ma span={span_minutes:.2f} minuti")

    # VALIDAZIONE 4: get_last_n_minutes dovrebbe avere span ≤ n minuti
    if get_last_n_minutes:
        span_minutes = (time_max - time_min).total_seconds() / 60
        if span_minutes > get_last_n_minutes + 0.1:
            issues.append(f"❌ INCOERENTE: get_last_n_minutes={get_last_n_minutes} ma span={span_minutes:.2f} minuti")

    # VALIDAZIONE 5: start_date - dati devono iniziare >= start_date
    if start_date_str:
        start_dt = pd.to_datetime(start_date_str).date()
        if time_min.date() < start_dt:
            issues.append(f"❌ INCOERENTE: start_date={start_dt} ma dati iniziano il {time_min.date()}")

    # VALIDAZIONE 6: end_date - dati devono finire <= end_date
    if end_date_str:
        end_dt = pd.to_datetime(end_date_str).date()
        if time_max.date() > end_dt:
            issues.append(f"❌ INCOERENTE: end_date={end_dt} ma dati finiscono il {time_max.date()}")

    return issues


def display_result(test_num, description, df, warnings, expected_behavior,
                  start_date_str=None, end_date_str=None,
                  get_first_n_minutes=None, get_last_n_minutes=None,
                  get_first_n_days=None, get_last_n_days=None):
    """Display test result with validation."""
    log_console(f"\n{'='*80}")
    log_console(f"TEST {test_num}: {description}")
    log_console(f"Comportamento atteso: {expected_behavior}")
    log_console(f"{'='*80}")

    if warnings:
        log_console(f"⚠️  Warnings: {warnings}")

    if df is None or len(df) == 0:
        if any('INVALID_MINUTES_PARAM' in w for w in warnings):
            log_console("✅ OK: Parametro invalido correttamente rifiutato")
            return True
        log_console("❌ FAIL: Nessun dato restituito")
        return False

    # Validate logic
    issues = validate_result(
        description, df, warnings,
        start_date_str, end_date_str,
        get_first_n_minutes, get_last_n_minutes,
        get_first_n_days, get_last_n_days
    )

    log_console(f"✅ OK: {len(df)} righe restituite")

    # Show time range
    time_col = None
    for col in ['timestamp', 'ms_of_day', 'date']:
        if col in df.columns:
            time_col = col
            break

    if time_col:
        df_copy = df.copy()
        df_copy[time_col] = pd.to_datetime(df_copy[time_col])

        time_min = df_copy[time_col].min()
        time_max = df_copy[time_col].max()
        unique_timestamps = df_copy[time_col].nunique()

        time_min_str = time_min.strftime('%Y-%m-%d %H:%M:%S.%f')
        time_max_str = time_max.strftime('%Y-%m-%d %H:%M:%S.%f')

        log_console(f"   Prima riga timestamp: {time_min_str}")
        log_console(f"   Ultima riga timestamp: {time_max_str}")
        log_console(f"   Span temporale: {time_max - time_min}")
        log_console(f"   Timestamp unici: {unique_timestamps} su {len(df)} righe")

        if unique_timestamps == 1:
            log_console(f"   ⚠️  Tutte le righe hanno lo stesso timestamp (diverse opzioni)")

    # Display validation issues
    if issues:
        for issue in issues:
            log_console(f"   {issue}")
        return False  # Mark as FAIL if validation fails

    return True


async def run_tests():
    """Run all comprehensive tests."""
    async with ThetaDataV3Client(base_url='http://localhost:25503/v3') as client:
        manager = ThetaSyncManager(cfg, client)

        log_console("="*80)
        log_console("COMPREHENSIVE QUERY TESTS - ALL INTERVALS, ALL SINKS")
        log_console("Fixed version: Handles days without data (weekends, holidays)")
        log_console("="*80)

        # Get available data
        log_console("\n📊 Scansione dati disponibili...")
        available = manager.list_available_data()
        log_console(f"✅ Trovate {len(available)} serie di dati\n")

        # Group by interval type
        tick_data = available[available['interval'] == 'tick']
        minute_data = available[available['interval'].isin(['1m', '5m', '10m', '15m', '30m', '1h'])]
        daily_data = available[available['interval'] == '1d']

        log_console(f"📈 Dati disponibili per intervallo:")
        log_console(f"   • Tick: {len(tick_data)} serie")
        log_console(f"   • Minuti (1m-1h): {len(minute_data)} serie")
        log_console(f"   • 1d: {len(daily_data)} serie")

        all_results = []

        # ============================================================================
        # TEST SUITE FOR EACH INTERVAL TYPE
        # ============================================================================

        for interval_name, interval_df in [
            ('TICK', tick_data),
            ('MINUTES', minute_data),
            ('DAILY (1d)', daily_data)
        ]:
            if interval_df.empty:
                log_console(f"\n⚠️  Nessun dato {interval_name} disponibile, salto...")
                continue

            log_console(f"\n{'#'*80}")
            log_console(f"# TESTING INTERVAL: {interval_name}")
            log_console(f"{'#'*80}")

            # For each interval type, test across all sinks
            for sink in ['csv', 'parquet', 'influxdb']:
                sink_data = interval_df[interval_df['sink'] == sink]

                if sink_data.empty:
                    log_console(f"\n⚠️  Nessun dato {interval_name} su sink={sink}, salto...")
                    continue

                # Get first series for this interval+sink combination
                row = sink_data.iloc[0]
                asset = row['asset']
                symbol = row['symbol']
                interval = row['interval']

                first_dt = normalize_tz(pd.to_datetime(row['first_datetime']))
                last_dt = normalize_tz(pd.to_datetime(row['last_datetime']))

                log_console(f"\n{'='*80}")
                log_console(f"SERIE: {symbol} ({asset}) - {interval} - sink={sink}")
                log_console(f"{'='*80}")
                log_console(f"📅 Range completo (metadata): {first_dt.date()} → {last_dt.date()}")

                # ================================================================
                # NEW: Get actual trading days with data
                # ================================================================
                trading_days = get_available_trading_days(manager, asset, symbol, interval, sink)

                if len(trading_days) < 2:
                    log_console(f"⚠️  Meno di 2 giorni con dati disponibili, salto questa serie")
                    continue

                # Use second available day as start_date, second-to-last as end_date
                # This ensures we're NOT using the first or last day
                if len(trading_days) >= 3:
                    start_date_dt = trading_days[1]  # Second day with data
                    end_date_dt = trading_days[-2]   # Second-to-last day with data
                else:
                    # Only 2 days available, use them
                    start_date_dt = trading_days[0]
                    end_date_dt = trading_days[-1]

                start_date_str = start_date_dt.strftime("%Y-%m-%d")
                end_date_str = end_date_dt.strftime("%Y-%m-%d")

                log_console(f"📅 start_date scelto: {start_date_str} (giorno con dati confermati)")
                log_console(f"📅 end_date scelto: {end_date_str} (giorno con dati confermati)")

                results = []
                test_num = 1

                # ====================================================================
                # SCENARIO 1: SOLO start_date
                # ====================================================================
                log_console(f"\n{'─'*80}")
                log_console(f"SCENARIO 1: SOLO start_date = {start_date_str}")
                log_console(f"{'─'*80}")

                # Test 1.1: get_first_n_rows
                df, warn = manager.query_local_data(
                    asset=asset, symbol=symbol, interval=interval, sink=sink,
                    start_date=start_date_str,
                    get_first_n_rows=5
                )
                success = display_result(
                    f"{test_num}",
                    f"start_date + get_first_n_rows=5",
                    df, warn,
                    "Prime 5 righe cronologiche dalla start_date",
                    start_date_str=start_date_str
                )
                results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                test_num += 1

                # Test 1.2: get_last_n_rows
                df, warn = manager.query_local_data(
                    asset=asset, symbol=symbol, interval=interval, sink=sink,
                    start_date=start_date_str,
                    get_last_n_rows=5
                )
                success = display_result(
                    f"{test_num}",
                    f"start_date + get_last_n_rows=5",
                    df, warn,
                    "Ultime 5 righe cronologiche (più recenti da TUTTI i dati)",
                    start_date_str=start_date_str
                )
                results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                test_num += 1

                # Test 1.3: get_first_n_days
                df, warn = manager.query_local_data(
                    asset=asset, symbol=symbol, interval=interval, sink=sink,
                    start_date=start_date_str,
                    get_first_n_days=1
                )
                success = display_result(
                    f"{test_num}",
                    f"start_date + get_first_n_days=1",
                    df, warn,
                    "Primo giorno dalla start_date",
                    start_date_str=start_date_str,
                    get_first_n_days=1
                )
                results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                test_num += 1

                # Test 1.4: get_first_n_minutes (solo per tick/minuti)
                if interval in ['tick', '1s', '5s', '10s', '15s', '30s', '1m', '5m', '10m', '15m', '30m', '1h']:
                    df, warn = manager.query_local_data(
                        asset=asset, symbol=symbol, interval=interval, sink=sink,
                        start_date=start_date_str,
                        get_first_n_minutes=5
                    )
                    success = display_result(
                        f"{test_num}",
                        f"start_date + get_first_n_minutes=5",
                        df, warn,
                        "Primi 5 minuti dalla start_date",
                        start_date_str=start_date_str,
                        get_first_n_minutes=5
                    )
                    results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                else:
                    # For 1d data, get_first_n_minutes should be rejected with warning
                    df, warn = manager.query_local_data(
                        asset=asset, symbol=symbol, interval=interval, sink=sink,
                        start_date=start_date_str,
                        get_first_n_minutes=5
                    )
                    success = display_result(
                        f"{test_num}",
                        f"start_date + get_first_n_minutes=5 (1d - should be rejected)",
                        df, warn,
                        "Parametro invalido - warning INVALID_MINUTES_PARAM"
                    )
                    results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))

                test_num += 1

                # ====================================================================
                # SCENARIO 2: SOLO end_date
                # ====================================================================
                log_console(f"\n{'─'*80}")
                log_console(f"SCENARIO 2: SOLO end_date = {end_date_str}")
                log_console(f"{'─'*80}")

                # Test 2.1: get_first_n_rows
                df, warn = manager.query_local_data(
                    asset=asset, symbol=symbol, interval=interval, sink=sink,
                    end_date=end_date_str,
                    get_first_n_rows=5
                )
                success = display_result(
                    f"{test_num}",
                    f"end_date + get_first_n_rows=5",
                    df, warn,
                    "Prime 5 righe cronologiche fino alla end_date",
                    end_date_str=end_date_str
                )
                results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                test_num += 1

                # Test 2.2: get_last_n_rows
                df, warn = manager.query_local_data(
                    asset=asset, symbol=symbol, interval=interval, sink=sink,
                    end_date=end_date_str,
                    get_last_n_rows=5
                )
                success = display_result(
                    f"{test_num}",
                    f"end_date + get_last_n_rows=5",
                    df, warn,
                    "Ultime 5 righe cronologiche fino alla end_date",
                    end_date_str=end_date_str
                )
                results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                test_num += 1

                # Test 2.3: get_last_n_days
                df, warn = manager.query_local_data(
                    asset=asset, symbol=symbol, interval=interval, sink=sink,
                    end_date=end_date_str,
                    get_last_n_days=1
                )
                success = display_result(
                    f"{test_num}",
                    f"end_date + get_last_n_days=1",
                    df, warn,
                    "Ultimo giorno fino alla end_date",
                    end_date_str=end_date_str,
                    get_last_n_days=1
                )
                results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                test_num += 1

                # Test 2.4: get_last_n_minutes (solo per tick/minuti)
                if interval in ['tick', '1s', '5s', '10s', '15s', '30s', '1m', '5m', '10m', '15m', '30m', '1h']:
                    df, warn = manager.query_local_data(
                        asset=asset, symbol=symbol, interval=interval, sink=sink,
                        end_date=end_date_str,
                        get_last_n_minutes=5
                    )
                    success = display_result(
                        f"{test_num}",
                        f"end_date + get_last_n_minutes=5",
                        df, warn,
                        "Ultimi 5 minuti fino alla end_date",
                        end_date_str=end_date_str,
                        get_last_n_minutes=5
                    )
                    results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                else:
                    df, warn = manager.query_local_data(
                        asset=asset, symbol=symbol, interval=interval, sink=sink,
                        end_date=end_date_str,
                        get_last_n_minutes=5
                    )
                    success = display_result(
                        f"{test_num}",
                        f"end_date + get_last_n_minutes=5 (1d - should be rejected)",
                        df, warn,
                        "Parametro invalido - warning INVALID_MINUTES_PARAM"
                    )
                    results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))

                test_num += 1

                # ====================================================================
                # SCENARIO 3: SIA start_date CHE end_date
                # ====================================================================
                if len(trading_days) >= 3:  # Need at least 3 days for proper range
                    log_console(f"\n{'─'*80}")
                    log_console(f"SCENARIO 3: start_date={start_date_str} + end_date={end_date_str}")
                    log_console(f"{'─'*80}")

                    # Test 3.1: get_first_n_rows
                    df, warn = manager.query_local_data(
                        asset=asset, symbol=symbol, interval=interval, sink=sink,
                        start_date=start_date_str,
                        end_date=end_date_str,
                        get_first_n_rows=10
                    )
                    success = display_result(
                        f"{test_num}",
                        f"start + end + get_first_n_rows=10",
                        df, warn,
                        "Prime 10 righe nel range start-end",
                        start_date_str=start_date_str,
                        end_date_str=end_date_str
                    )
                    results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                    test_num += 1

                    # Test 3.2: get_last_n_rows
                    df, warn = manager.query_local_data(
                        asset=asset, symbol=symbol, interval=interval, sink=sink,
                        start_date=start_date_str,
                        end_date=end_date_str,
                        get_last_n_rows=10
                    )
                    success = display_result(
                        f"{test_num}",
                        f"start + end + get_last_n_rows=10",
                        df, warn,
                        "Ultime 10 righe nel range start-end",
                        start_date_str=start_date_str,
                        end_date_str=end_date_str
                    )
                    results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                    test_num += 1

                    # Test 3.3: get_first_n_days
                    df, warn = manager.query_local_data(
                        asset=asset, symbol=symbol, interval=interval, sink=sink,
                        start_date=start_date_str,
                        end_date=end_date_str,
                        get_first_n_days=1
                    )
                    success = display_result(
                        f"{test_num}",
                        f"start + end + get_first_n_days=1",
                        df, warn,
                        "Primo giorno nel range start-end",
                        start_date_str=start_date_str,
                        end_date_str=end_date_str,
                        get_first_n_days=1
                    )
                    results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                    test_num += 1

                    # Test 3.4: get_last_n_days
                    df, warn = manager.query_local_data(
                        asset=asset, symbol=symbol, interval=interval, sink=sink,
                        start_date=start_date_str,
                        end_date=end_date_str,
                        get_last_n_days=1
                    )
                    success = display_result(
                        f"{test_num}",
                        f"start + end + get_last_n_days=1",
                        df, warn,
                        "Ultimo giorno nel range start-end",
                        start_date_str=start_date_str,
                        end_date_str=end_date_str,
                        get_last_n_days=1
                    )
                    results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                    test_num += 1

                # ====================================================================
                # SCENARIO 4: NESSUNA data (comportamento default)
                # ====================================================================
                log_console(f"\n{'─'*80}")
                log_console(f"SCENARIO 4: NESSUNA data (default)")
                log_console(f"{'─'*80}")

                # Test 4.1: get_first_n_rows (no dates)
                df, warn = manager.query_local_data(
                    asset=asset, symbol=symbol, interval=interval, sink=sink,
                    get_first_n_rows=5
                )
                success = display_result(
                    f"{test_num}",
                    f"no dates + get_first_n_rows=5",
                    df, warn,
                    "Prime 5 righe di TUTTI i dati"
                )
                results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                test_num += 1

                # Test 4.2: get_last_n_rows (no dates)
                df, warn = manager.query_local_data(
                    asset=asset, symbol=symbol, interval=interval, sink=sink,
                    get_last_n_rows=5
                )
                success = display_result(
                    f"{test_num}",
                    f"no dates + get_last_n_rows=5",
                    df, warn,
                    "Ultime 5 righe di TUTTI i dati (più recenti)"
                )
                results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                test_num += 1

                # Test 4.3: get_first_n_days (no dates)
                df, warn = manager.query_local_data(
                    asset=asset, symbol=symbol, interval=interval, sink=sink,
                    get_first_n_days=1
                )
                success = display_result(
                    f"{test_num}",
                    f"no dates + get_first_n_days=1",
                    df, warn,
                    "Primo giorno di TUTTI i dati",
                    get_first_n_days=1
                )
                results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                test_num += 1

                # Test 4.4: get_last_n_days (no dates)
                df, warn = manager.query_local_data(
                    asset=asset, symbol=symbol, interval=interval, sink=sink,
                    get_last_n_days=1
                )
                success = display_result(
                    f"{test_num}",
                    f"no dates + get_last_n_days=1",
                    df, warn,
                    "Ultimo giorno di TUTTI i dati (più recente)",
                    get_last_n_days=1
                )
                results.append((f'TEST {test_num}', interval_name, sink, 'PASS' if success else 'FAIL'))
                test_num += 1

                # Store results for this series
                all_results.extend(results)

        # ====================================================================
        # FINAL SUMMARY
        # ====================================================================
        log_console(f"\n{'='*80}")
        log_console(f"RIEPILOGO FINALE - TUTTI I TEST")
        log_console(f"{'='*80}\n")

        # Create summary DataFrame
        df_summary = pd.DataFrame(all_results, columns=['Test', 'Interval', 'Sink', 'Status'])
        log_console(df_summary.to_string(index=False))

        total = len(all_results)
        passed = len([r for r in all_results if r[3] == 'PASS'])
        failed = len([r for r in all_results if r[3] == 'FAIL'])

        log_console(f"\n{'='*80}")
        log_console(f"Total tests: {total}")
        log_console(f"✅ Passed: {passed}")
        log_console(f"❌ Failed: {failed}")
        log_console(f"Success rate: {100 * passed / total:.1f}%")
        log_console(f"{'='*80}")

# Run the tests
await run_tests()
