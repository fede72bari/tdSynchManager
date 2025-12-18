import sys
import os
sys.path.insert(0, r'd:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src')

from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig
import pandas as pd

print("=== TEST 1: Create manager ===")
cfg = ManagerConfig(
    root_dir=r"tests/data",
    max_concurrency=1
)
manager = ThetaSyncManager(cfg, client=None)
print("OK Manager created")

print("\n=== TEST 2: Check method signature ===")
import inspect
sig = inspect.signature(manager._list_series_files)
print(f"Method signature: {sig}")
params = list(sig.parameters.keys())
print(f"Parameters: {params}")
assert 'sink_lower' in params, "sink_lower parameter missing!"
print("OK Correct signature confirmed")

print("\n=== TEST 3: List files ===")
files = manager._list_series_files(
    asset="option",
    symbol="QQQ",
    interval="5m",
    sink_lower="csv"
)
print(f"Found {len(files)} files")
if files:
    print(f"First file: {os.path.basename(files[0])}")
    print("OK Files listed successfully")
else:
    print("WARNING: No files found (might be expected if no data downloaded)")

print("\n=== TEST 4: Read sample file and show columns ===")
if files:
    sample_file = files[0]
    df = pd.read_csv(sample_file)
    print(f"Sample file: {os.path.basename(sample_file)}")
    print(f"Rows: {len(df):,}, Columns: {len(df.columns)}")
    print(f"\nAll columns ({len(df.columns)}):")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i:2d}. {col}")
    print("OK File read successfully")
else:
    print("WARNING: Skipped - no files to read")

print("\n=== TEST 5: Extract last row ===")
if files:
    last_row = df.iloc[-1]
    print(f"Last row extracted: {len(last_row)} fields")
    if 'timestamp' in last_row:
        print(f"Last timestamp: {last_row['timestamp']}")
    print("OK Last row extraction works")
else:
    print("WARNING: Skipped - no files to read")

print("\n=== ALL TESTS PASSED ===")
