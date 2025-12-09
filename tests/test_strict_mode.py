"""
Test script per verificare il comportamento strict mode per le opzioni.

Questo script testa:
1. EOD: skip save se greeks/OI falliscono quando enrich_greeks=True
2. Intraday: skip expirations se greeks/IV falliscono quando enrich_greeks=True
3. Logging strutturato di errori/warning nel DataConsistencyLogger

NOTA: Questo è un test simulato che NON effettua chiamate reali all'API ThetaData.
      Serve solo per verificare la logica di skip e logging.
"""

import asyncio
from datetime import datetime


def test_eod_strict_mode():
    """Test EOD strict mode behavior."""
    print("\n=== TEST 1: EOD Strict Mode ===")
    print("Scenario: enrich_greeks=True, ma greeks download fallisce")
    print("Atteso: Skip save completo + log ERROR")
    print("\nPer testare realmente:")
    print("1. Avvia ThetaData Terminal")
    print("2. Usa manager.run_option_eod_one_day() con enrich_greeks=True")
    print("3. Su un simbolo/data che causa HTTP 500 su greeks endpoint")
    print("4. Verifica output console per '[SKIP-SAVE]' messages")
    print("5. Verifica che NON ci sia file salvato per quella data")
    print("6. Verifica log parquet per event_type='GREEKS_DOWNLOAD_FAILED'")


def test_intraday_strict_mode():
    """Test Intraday strict mode behavior."""
    print("\n=== TEST 2: Intraday Strict Mode ===")
    print("Scenario: enrich_greeks=True, alcune expirations falliscono greeks/IV")
    print("Atteso: Skip solo quelle expirations + log WARNING per ogni skip")
    print("\nPer testare realmente:")
    print("1. Avvia ThetaData Terminal")
    print("2. Usa manager.run_option_intraday_one_day() con enrich_greeks=True")
    print("3. Su AAL 2022-01-03 5m (noto per avere errori HTTP 500)")
    print("4. Verifica output console:")
    print("   - '[SKIP-EXPIRATION]' per expirations che falliscono")
    print("   - '[SKIP-DAY]' se TUTTE le expirations falliscono")
    print("5. Verifica che solo expirations con greeks/IV success siano salvate")
    print("6. Verifica log parquet per event_type='GREEKS_DOWNLOAD_FAILED_INTRADAY'")


def test_logger_methods():
    """Test DataConsistencyLogger new methods."""
    print("\n=== TEST 3: Logger Methods ===")
    print("Verifico che i nuovi metodi log_error() e log_warning() esistano...")

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
        print("[OK] log_error() method works")

        # Test log_warning
        logger.log_warning(
            asset="option",
            symbol="TEST",
            interval="5m",
            date="2024-01-01",
            warning_type="TEST_WARNING",
            warning_message="Test warning message"
        )
        print("[OK] log_warning() method works")

        print("\n[OK] Tutti i metodi del logger funzionano correttamente!")

    except Exception as e:
        print(f"[ERROR] Errore nel test logger: {e}")
        import traceback
        traceback.print_exc()


def print_example_output():
    """Mostra esempi di output attesi."""
    print("\n" + "="*70)
    print("ESEMPI DI OUTPUT ATTESI")
    print("="*70)

    print("\n--- Console Output (EOD skip) ---")
    print("[SKIP-SAVE] option EOD AAL 2022-01-03: greeks download failed, skipping to allow coherence check detection")
    print("[VALIDATION] STRICT MODE: Skipping save for option AAL EOD 2022-01-03 - all retry attempts failed")

    print("\n--- Console Output (Intraday skip expiration) ---")
    print("[SKIP-EXPIRATION] option intraday AAL 5m 2022-01-03 exp=20220916: greeks download failed")
    print("[SKIP-EXPIRATION] option intraday AAL 5m 2022-01-03 exp=20220916: IV download failed")
    print("[SKIP-EXPIRATION] option intraday AAL 5m 2022-01-03 exp=20220916: OHLC not saved due to incomplete greeks/IV")

    print("\n--- Console Output (Intraday skip day) ---")
    print("[SKIP-DAY] option intraday AAL 5m 2022-01-03: no data saved (all expirations failed/skipped)")

    print("\n--- Log Parquet Schema ---")
    print("timestamp          | event_type                    | severity | symbol | error_message")
    print("2024-01-01 10:00:00 | GREEKS_DOWNLOAD_FAILED       | ERROR    | AAL    | Greeks EOD download returned empty/None")
    print("2024-01-01 10:00:05 | GREEKS_CSV_EMPTY             | ERROR    | AAL    | Greeks EOD CSV parsed to empty DataFrame")
    print("2024-01-01 10:00:10 | OI_DOWNLOAD_FAILED           | ERROR    | AAL    | Open Interest download for date 20220103")
    print("2024-01-01 10:05:00 | GREEKS_DOWNLOAD_FAILED_INTRADAY | WARNING | AAL | Greeks download failed for expiration 20220916")


def print_verification_steps():
    """Mostra passi per verificare le modifiche."""
    print("\n" + "="*70)
    print("PASSI PER VERIFICARE LE MODIFICHE")
    print("="*70)

    print("\n1. Verifica Syntax:")
    print("   cd d:\\Dropbox\\TRADING\\DATA FEEDERS AND APIS\\ThetaData\\tdSynchManager")
    print("   /c/Users/Federico/anaconda3/python.exe -m py_compile src/tdSynchManager/manager.py")
    print("   /c/Users/Federico/anaconda3/python.exe -m py_compile src/tdSynchManager/logger.py")

    print("\n2. Test EOD con AAL 2022-01-03:")
    print("   python -c \"")
    print("   import asyncio")
    print("   from src.tdSynchManager import ThetaSyncManager")
    print("   async def test():")
    print("       mgr = ThetaSyncManager('thetadata_config.yaml')")
    print("       await mgr.run_option_eod_one_day(")
    print("           symbol='AAL',")
    print("           day_iso='2022-01-03',")
    print("           enrich_greeks=True,")
    print("           sink='csv'  # oppure 'influxdb'")
    print("       )")
    print("   asyncio.run(test())")
    print("   \"")

    print("\n3. Test Intraday con AAL 2022-01-03:")
    print("   python -c \"")
    print("   import asyncio")
    print("   from src.tdSynchManager import ThetaSyncManager")
    print("   async def test():")
    print("       mgr = ThetaSyncManager('thetadata_config.yaml')")
    print("       await mgr.run_option_intraday_one_day(")
    print("           symbol='AAL',")
    print("           interval='5m',")
    print("           day_iso='2022-01-03',")
    print("           enrich_greeks=True,")
    print("           sink='csv'")
    print("       )")
    print("   asyncio.run(test())")
    print("   \"")

    print("\n4. Verifica Log Parquet:")
    print("   python -c \"")
    print("   import pandas as pd")
    print("   import glob")
    print("   files = glob.glob('./data/option/AAL/*/logs/*.parquet')")
    print("   for f in files:")
    print("       df = pd.read_parquet(f)")
    print("       print(df[['timestamp', 'event_type', 'severity', 'error_message']].tail(10))")
    print("   \"")

    print("\n5. Verifica File Salvati:")
    print("   # Se strict mode funziona, NON dovrebbe esistere file per 2022-01-03 se greeks falliscono")
    print("   ls ./data/option/AAL/1d/2022-01-03*  # dovrebbe essere vuoto se skip")
    print("   ls ./data/option/AAL/5m/2022-01-03*  # potrebbe avere solo alcune expirations")


def main():
    """Main test runner."""
    print("="*70)
    print("TEST STRICT MODE - OPTION GREEKS/IV/OI")
    print("="*70)

    test_eod_strict_mode()
    test_intraday_strict_mode()
    test_logger_methods()
    print_example_output()
    print_verification_steps()

    print("\n" + "="*70)
    print("TEST COMPLETATO")
    print("="*70)
    print("\nRicorda: I test 1 e 2 richiedono ThetaData Terminal attivo e chiamate reali.")
    print("Usa i comandi sopra per testare con dati reali.\n")


if __name__ == "__main__":
    main()
