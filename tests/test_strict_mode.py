"""
Test script per verificare il comportamento strict mode per le opzioni.

Questo script testa:
1. EOD: skip save se greeks/OI falliscono quando enrich_greeks=True
2. Intraday: skip expirations se greeks/IV falliscono quando enrich_greeks=True
3. Logging strutturato di errori/warning nel DataConsistencyLogger

NOTA: Questo è un test simulato che NON effettua chiamate reali all'API ThetaData.
      Serve solo per verificare la logica di skip e logging.
"""
from console_log import log_console

import asyncio
from datetime import datetime


def test_eod_strict_mode():
    """Test EOD strict mode behavior."""
    log_console("\n=== TEST 1: EOD Strict Mode ===")
    log_console("Scenario: enrich_greeks=True, ma greeks download fallisce")
    log_console("Atteso: Skip save completo + log ERROR")
    log_console("\nPer testare realmente:")
    log_console("1. Avvia ThetaData Terminal")
    log_console("2. Usa manager.run_option_eod_one_day() con enrich_greeks=True")
    log_console("3. Su un simbolo/data che causa HTTP 500 su greeks endpoint")
    log_console("4. Verifica output console per '[SKIP-SAVE]' messages")
    log_console("5. Verifica che NON ci sia file salvato per quella data")
    log_console("6. Verifica log parquet per event_type='GREEKS_DOWNLOAD_FAILED'")


def test_intraday_strict_mode():
    """Test Intraday strict mode behavior."""
    log_console("\n=== TEST 2: Intraday Strict Mode ===")
    log_console("Scenario: enrich_greeks=True, alcune expirations falliscono greeks/IV")
    log_console("Atteso: Skip solo quelle expirations + log WARNING per ogni skip")
    log_console("\nPer testare realmente:")
    log_console("1. Avvia ThetaData Terminal")
    log_console("2. Usa manager.run_option_intraday_one_day() con enrich_greeks=True")
    log_console("3. Su AAL 2022-01-03 5m (noto per avere errori HTTP 500)")
    log_console("4. Verifica output console:")
    log_console("   - '[SKIP-EXPIRATION]' per expirations che falliscono")
    log_console("   - '[SKIP-DAY]' se TUTTE le expirations falliscono")
    log_console("5. Verifica che solo expirations con greeks/IV success siano salvate")
    log_console("6. Verifica log parquet per event_type='GREEKS_DOWNLOAD_FAILED_INTRADAY'")


def test_logger_methods():
    """Test DataConsistencyLogger new methods."""
    log_console("\n=== TEST 3: Logger Methods ===")
    log_console("Verifico che i nuovi metodi log_error() e log_warning() esistano...")

    try:
        from src.tdSynchManager.logger import DataConsistencyLogger

        logger = DataConsistencyLogger(root_dir="./test_data", verbose_console=False)

        # Test log_error
        logger.log_error(
            asset="option",
            symbol="TEST",
            interval="5m",
            date="2024-01-01",
            error_type="TEST_ERROR",
            error_message="Test error message",
            severity="ERROR"
        )
        log_console("[OK] log_error() method works")

        # Test log_warning
        logger.log_warning(
            asset="option",
            symbol="TEST",
            interval="5m",
            date="2024-01-01",
            warning_type="TEST_WARNING",
            warning_message="Test warning message"
        )
        log_console("[OK] log_warning() method works")

        log_console("\n[OK] Tutti i metodi del logger funzionano correttamente!")

    except Exception as e:
        log_console(f"[ERROR] Errore nel test logger: {e}")
        import traceback
        traceback.print_exc()


def print_example_output():
    """Mostra esempi di output attesi."""
    log_console("\n" + "="*70)
    log_console("ESEMPI DI OUTPUT ATTESI")
    log_console("="*70)

    log_console("\n--- Console Output (EOD skip) ---")
    log_console("[SKIP-SAVE] option EOD AAL 2022-01-03: greeks download failed, skipping to allow coherence check detection")
    log_console("[VALIDATION] STRICT MODE: Skipping save for option AAL EOD 2022-01-03 - all retry attempts failed")

    log_console("\n--- Console Output (Intraday skip expiration) ---")
    log_console("[SKIP-EXPIRATION] option intraday AAL 5m 2022-01-03 exp=20220916: greeks download failed")
    log_console("[SKIP-EXPIRATION] option intraday AAL 5m 2022-01-03 exp=20220916: IV download failed")
    log_console("[SKIP-EXPIRATION] option intraday AAL 5m 2022-01-03 exp=20220916: OHLC not saved due to incomplete greeks/IV")

    log_console("\n--- Console Output (Intraday skip day) ---")
    log_console("[SKIP-DAY] option intraday AAL 5m 2022-01-03: no data saved (all expirations failed/skipped)")

    log_console("\n--- Log Parquet Schema ---")
    log_console("timestamp          | event_type                    | severity | symbol | error_message")
    log_console("2024-01-01 10:00:00 | GREEKS_DOWNLOAD_FAILED       | ERROR    | AAL    | Greeks EOD download returned empty/None")
    log_console("2024-01-01 10:00:05 | GREEKS_CSV_EMPTY             | ERROR    | AAL    | Greeks EOD CSV parsed to empty DataFrame")
    log_console("2024-01-01 10:00:10 | OI_DOWNLOAD_FAILED           | ERROR    | AAL    | Open Interest download for date 20220103")
    log_console("2024-01-01 10:05:00 | GREEKS_DOWNLOAD_FAILED_INTRADAY | WARNING | AAL | Greeks download failed for expiration 20220916")


def print_verification_steps():
    """Mostra passi per verificare le modifiche."""
    log_console("\n" + "="*70)
    log_console("PASSI PER VERIFICARE LE MODIFICHE")
    log_console("="*70)

    log_console("\n1. Verifica Syntax:")
    log_console("   cd d:\\Dropbox\\TRADING\\DATA FEEDERS AND APIS\\ThetaData\\tdSynchManager")
    log_console("   /c/Users/Federico/anaconda3/python.exe -m py_compile src/tdSynchManager/manager.py")
    log_console("   /c/Users/Federico/anaconda3/python.exe -m py_compile src/tdSynchManager/logger.py")

    log_console("\n2. Test EOD con AAL 2022-01-03:")
    log_console("   python -c \"")
    log_console("   import asyncio")
    log_console("   from src.tdSynchManager import ThetaSyncManager")
    log_console("   async def test():")
    log_console("       mgr = ThetaSyncManager('thetadata_config.yaml')")
    log_console("       await mgr.run_option_eod_one_day(")
    log_console("           symbol='AAL',")
    log_console("           day_iso='2022-01-03',")
    log_console("           enrich_greeks=True,")
    log_console("           sink='csv'  # oppure 'influxdb'")
    log_console("       )")
    log_console("   asyncio.run(test())")
    log_console("   \"")

    log_console("\n3. Test Intraday con AAL 2022-01-03:")
    log_console("   python -c \"")
    log_console("   import asyncio")
    log_console("   from src.tdSynchManager import ThetaSyncManager")
    log_console("   async def test():")
    log_console("       mgr = ThetaSyncManager('thetadata_config.yaml')")
    log_console("       await mgr.run_option_intraday_one_day(")
    log_console("           symbol='AAL',")
    log_console("           interval='5m',")
    log_console("           day_iso='2022-01-03',")
    log_console("           enrich_greeks=True,")
    log_console("           sink='csv'")
    log_console("       )")
    log_console("   asyncio.run(test())")
    log_console("   \"")

    log_console("\n4. Verifica Log Parquet:")
    log_console("   python -c \"")
    log_console("   import pandas as pd")
    log_console("   import glob")
    log_console("   files = glob.glob('./data/option/AAL/*/logs/*.parquet')")
    log_console("   for f in files:")
    log_console("       df = pd.read_parquet(f)")
    log_console("       print(df[['timestamp', 'event_type', 'severity', 'error_message']].tail(10))")
    log_console("   \"")

    log_console("\n5. Verifica File Salvati:")
    log_console("   # Se strict mode funziona, NON dovrebbe esistere file per 2022-01-03 se greeks falliscono")
    log_console("   ls ./data/option/AAL/1d/2022-01-03*  # dovrebbe essere vuoto se skip")
    log_console("   ls ./data/option/AAL/5m/2022-01-03*  # potrebbe avere solo alcune expirations")


def main():
    """Main test runner."""
    log_console("="*70)
    log_console("TEST STRICT MODE - OPTION GREEKS/IV/OI")
    log_console("="*70)

    test_eod_strict_mode()
    test_intraday_strict_mode()
    test_logger_methods()
    print_example_output()
    print_verification_steps()

    log_console("\n" + "="*70)
    log_console("TEST COMPLETATO")
    log_console("="*70)
    log_console("\nRicorda: I test 1 e 2 richiedono ThetaData Terminal attivo e chiamate reali.")
    log_console("Usa i comandi sopra per testare con dati reali.\n")


if __name__ == "__main__":
    main()
