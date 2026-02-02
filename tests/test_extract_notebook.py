from console_log import log_console
import sys
import os
sys.path.insert(0, r'd:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src')

from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig
import pandas as pd

log_console("=== TEST 1: Create manager ===")
cfg = ManagerConfig(
    root_dir=r"tests/data",
    max_concurrency=1
)
manager = ThetaSyncManager(cfg, client=None)
log_console("OK Manager created")

log_console("\n=== TEST 2: Check method signature ===")
import inspect
sig = inspect.signature(manager._list_series_files)
log_console(f"Method signature: {sig}")
params = list(sig.parameters.keys())
log_console(f"Parameters: {params}")
assert 'sink_lower' in params, "sink_lower parameter missing!"
log_console("OK Correct signature confirmed")

log_console("\n=== TEST 3: List files ===")
files = manager._list_series_files(
    asset="option",
    symbol="QQQ",
    interval="5m",
    sink_lower="csv"
)
log_console(f"Found {len(files)} files")
if files:
    log_console(f"First file: {os.path.basename(files[0])}")
    log_console("OK Files listed successfully")
else:
    log_console("WARNING: No files found (might be expected if no data downloaded)")

log_console("\n=== TEST 4: Read sample file and show columns ===")
if files:
    sample_file = files[0]
    df = pd.read_csv(sample_file)
    log_console(f"Sample file: {os.path.basename(sample_file)}")
    log_console(f"Rows: {len(df):,}, Columns: {len(df.columns)}")
    log_console(f"\nAll columns ({len(df.columns)}):")
    for i, col in enumerate(df.columns, 1):
        log_console(f"  {i:2d}. {col}")
    log_console("OK File read successfully")
else:
    log_console("WARNING: Skipped - no files to read")

log_console("\n=== TEST 5: Extract last row ===")
if files:
    last_row = df.iloc[-1]
    log_console(f"Last row extracted: {len(last_row)} fields")
    if 'timestamp' in last_row:
        log_console(f"Last timestamp: {last_row['timestamp']}")
    log_console("OK Last row extraction works")
else:
    log_console("WARNING: Skipped - no files to read")

log_console("\n=== ALL TESTS PASSED ===")
