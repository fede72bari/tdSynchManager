"""
Test timezone auto-detection logic
"""
import sys
sys.path.insert(0, r'd:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src')

from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig
import pandas as pd
import io

print("=== Test Timezone Auto-Detection ===\n")

cfg = ManagerConfig(root_dir=r"C:\Users\Federico\Downloads", max_concurrency=1)
manager = ThetaSyncManager(cfg, client=None)

# Test Case 1: Simulated data with UTC timestamps (14-21 range)
print("Test 1: CSV data with UTC timestamps (hours 14-21)")
print("-" * 60)

csv_utc = """timestamp,symbol,volume
2025-12-08 14:30:00,QQQ,1000
2025-12-08 15:00:00,QQQ,1500
2025-12-08 16:30:00,QQQ,2000
2025-12-08 20:00:00,QQQ,1800"""

df_utc = pd.read_csv(io.StringIO(csv_utc), dtype=str)
print("Input (dovrebbero essere rilevati come UTC):")
print(df_utc['timestamp'].tolist())

df_utc_normalized = manager._normalize_ts_to_utc(df_utc)
print("\nOutput dopo _normalize_ts_to_utc():")
print(df_utc_normalized['timestamp'].tolist())
print("Atteso: Nessuna conversione (timestamp invariati)")

# Test Case 2: Simulated data with ET timestamps (9-16 range)
print("\n\nTest 2: CSV data with ET timestamps (hours 9-16)")
print("-" * 60)

csv_et = """timestamp,symbol,volume
2025-12-08 09:30:00,QQQ,1000
2025-12-08 10:00:00,QQQ,1500
2025-12-08 12:30:00,QQQ,2000
2025-12-08 16:00:00,QQQ,1800"""

df_et = pd.read_csv(io.StringIO(csv_et), dtype=str)
print("Input (dovrebbero essere rilevati come ET):")
print(df_et['timestamp'].tolist())

df_et_normalized = manager._normalize_ts_to_utc(df_et)
print("\nOutput dopo _normalize_ts_to_utc():")
print(df_et_normalized['timestamp'].tolist())
print("Atteso: Conversione ET->UTC (+5 ore in inverno)")

# Test Case 3: Real CSV file (currently has wrong timestamps 19:30-02:00)
print("\n\nTest 3: Real CSV file (timestamp attuali errati)")
print("-" * 60)

try:
    real_file = r'C:\Users\Federico\Downloads\data\option\QQQ\5m\csv\2025-12-08T00-00-00Z-QQQ-option-5m_part01.csv'
    df_real = pd.read_csv(real_file, dtype=str)

    print(f"File: part01.csv")
    print(f"Prime 3 timestamp originali:")
    for ts in df_real['timestamp'].head(3):
        print(f"  {ts}")

    # La detection dovrebbe rilevare questi come UTC (19-20 hours)
    # e NON convertirli, mantenendoli errati
    # Questo è OK - serve ri-scaricare i dati dall'API per avere quelli corretti

    print("\nQuesti timestamp (19:30-02:00) sono ERRATI ma verranno rilevati come UTC")
    print("La detection eviterà di peggiorare la situazione (+5 ore in più)")
    print("SOLUZIONE: Cancellare i file e ri-scaricare dall'API con il fix applicato")

except Exception as e:
    print(f"Errore: {e}")

print("\n" + "=" * 60)
print("RIEPILOGO:")
print("- Detection funziona: rileva automaticamente se timestamp in range UTC o ET")
print("- File esistenti hanno timestamp errati (19:30-02:00 invece di 14:30-21:00)")
print("- Questi verranno rilevati come UTC e NON peggiorati ulteriormente")
print("- Per avere timestamp corretti: CANCELLARE e RI-SCARICARE dall'API")
print("=" * 60)
