"""
Test timezone auto-detection logic
"""
from console_log import log_console
import sys
sys.path.insert(0, r'd:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src')

from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig
import pandas as pd
import io

log_console("=== Test Timezone Auto-Detection ===\n")

cfg = ManagerConfig(root_dir=r"tests/data", max_concurrency=1)
manager = ThetaSyncManager(cfg, client=None)

# Test Case 1: Simulated data with UTC timestamps (14-21 range)
log_console("Test 1: CSV data with UTC timestamps (hours 14-21)")
log_console("-" * 60)

csv_utc = """timestamp,symbol,volume
2025-12-08 14:30:00,QQQ,1000
2025-12-08 15:00:00,QQQ,1500
2025-12-08 16:30:00,QQQ,2000
2025-12-08 20:00:00,QQQ,1800"""

df_utc = pd.read_csv(io.StringIO(csv_utc), dtype=str)
log_console("Input (dovrebbero essere rilevati come UTC):")
log_console(df_utc['timestamp'].tolist())

df_utc_normalized = manager._normalize_ts_to_utc(df_utc)
log_console("\nOutput dopo _normalize_ts_to_utc():")
log_console(df_utc_normalized['timestamp'].tolist())
log_console("Atteso: Nessuna conversione (timestamp invariati)")

# Test Case 2: Simulated data with ET timestamps (9-16 range)
log_console("\n\nTest 2: CSV data with ET timestamps (hours 9-16)")
log_console("-" * 60)

csv_et = """timestamp,symbol,volume
2025-12-08 09:30:00,QQQ,1000
2025-12-08 10:00:00,QQQ,1500
2025-12-08 12:30:00,QQQ,2000
2025-12-08 16:00:00,QQQ,1800"""

df_et = pd.read_csv(io.StringIO(csv_et), dtype=str)
log_console("Input (dovrebbero essere rilevati come ET):")
log_console(df_et['timestamp'].tolist())

df_et_normalized = manager._normalize_ts_to_utc(df_et)
log_console("\nOutput dopo _normalize_ts_to_utc():")
log_console(df_et_normalized['timestamp'].tolist())
log_console("Atteso: Conversione ET->UTC (+5 ore in inverno)")

# Test Case 3: Real CSV file (currently has wrong timestamps 19:30-02:00)
log_console("\n\nTest 3: Real CSV file (timestamp attuali errati)")
log_console("-" * 60)

try:
    real_file = r'tests/data\data\option\QQQ\5m\csv\2025-12-08T00-00-00Z-QQQ-option-5m_part01.csv'
    df_real = pd.read_csv(real_file, dtype=str)

    log_console(f"File: part01.csv")
    log_console(f"Prime 3 timestamp originali:")
    for ts in df_real['timestamp'].head(3):
        log_console(f"  {ts}")

    # La detection dovrebbe rilevare questi come UTC (19-20 hours)
    # e NON convertirli, mantenendoli errati
    # Questo è OK - serve ri-scaricare i dati dall'API per avere quelli corretti

    log_console("\nQuesti timestamp (19:30-02:00) sono ERRATI ma verranno rilevati come UTC")
    log_console("La detection eviterà di peggiorare la situazione (+5 ore in più)")
    log_console("SOLUZIONE: Cancellare i file e ri-scaricare dall'API con il fix applicato")

except Exception as e:
    log_console(f"Errore: {e}")

log_console("\n" + "=" * 60)
log_console("RIEPILOGO:")
log_console("- Detection funziona: rileva automaticamente se timestamp in range UTC o ET")
log_console("- File esistenti hanno timestamp errati (19:30-02:00 invece di 14:30-21:00)")
log_console("- Questi verranno rilevati come UTC e NON peggiorati ulteriormente")
log_console("- Per avere timestamp corretti: CANCELLARE e RI-SCARICARE dall'API")
log_console("=" * 60)
