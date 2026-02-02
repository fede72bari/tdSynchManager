"""
Test to verify the timezone conversion fix.
This simulates the data flow through _normalize_ts_to_utc() and _normalize_df_types()
"""
from console_log import log_console
import sys
import os
sys.path.insert(0, r'd:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src')

from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig
import pandas as pd
import io

log_console("=== Test Timezone Conversion Fix ===\n")

# Create manager instance
cfg = ManagerConfig(root_dir=r"tests/data", max_concurrency=1)
manager = ThetaSyncManager(cfg, client=None)

# Test Case 1: Simulate API response with ET timestamps (expected format)
# Market open at 09:30 ET on 2025-12-08
log_console("Test Case 1: API returns ET timestamps")
log_console("-" * 60)

csv_data_et = """timestamp,symbol,strike,right,open,high,low,close,volume,bid,ask,underlying_price
2025-12-08 09:30:00,QQQ,500,call,1.50,1.60,1.45,1.55,1000,1.54,1.56,625.00
2025-12-08 16:00:00,QQQ,500,call,1.60,1.65,1.55,1.62,1500,1.61,1.63,625.50"""

df_et = pd.read_csv(io.StringIO(csv_data_et), dtype=str)
log_console("Original (simulating API response):")
log_console(df_et[['timestamp']].head())

# Apply normalization pipeline
df_et_normalized = manager._normalize_ts_to_utc(df_et)
log_console("\nAfter _normalize_ts_to_utc():")
log_console(df_et_normalized[['timestamp']].head())
log_console(f"Expected: 2025-12-08 14:30:00 (09:30 ET → 14:30 UTC)")

df_et_final = manager._normalize_df_types(df_et_normalized)
log_console("\nAfter _normalize_df_types():")
log_console(df_et_final[['timestamp']].head())
log_console(f"Expected: Should remain 2025-12-08 14:30:00 (no double conversion)")

# Check if correct
first_ts = pd.to_datetime(df_et_final['timestamp'].iloc[0])
expected_utc = pd.Timestamp("2025-12-08 14:30:00")
if first_ts == expected_utc:
    log_console("\n✓ PASS: Timestamps correctly converted from ET to UTC")
else:
    log_console(f"\n✗ FAIL: Expected {expected_utc}, got {first_ts}")
    log_console(f"   Difference: {(first_ts - expected_utc).total_seconds() / 3600:.1f} hours")

# Test Case 2: Read from existing CSV file with wrong timestamps
log_console("\n\n" + "=" * 60)
log_console("Test Case 2: Read existing CSV with incorrect timestamps")
log_console("-" * 60)

csv_files = manager._list_series_files(asset="option", symbol="QQQ", interval="5m", sink_lower="csv")
if csv_files:
    test_file = csv_files[0]
    log_console(f"Reading: {os.path.basename(test_file)}")

    df_csv = pd.read_csv(test_file)
    log_console("\nFirst 3 timestamps from CSV:")
    log_console(df_csv[['timestamp']].head(3))

    # Parse timestamps
    df_csv['timestamp_parsed'] = pd.to_datetime(df_csv['timestamp'])
    first_csv_ts = df_csv['timestamp_parsed'].iloc[0]
    last_csv_ts = df_csv['timestamp_parsed'].iloc[-1]

    log_console(f"\nFirst: {first_csv_ts}")
    log_console(f"Last: {last_csv_ts}")
    log_console(f"Expected range: 2025-12-08 14:30:00 to 2025-12-08 21:00:00 UTC")
    log_console(f"               (09:30-16:00 ET market hours)")

    # Check if in correct range
    expected_start = pd.Timestamp("2025-12-08 14:30:00")
    expected_end = pd.Timestamp("2025-12-08 21:00:00")

    start_diff = (first_csv_ts - expected_start).total_seconds() / 3600
    end_diff = (last_csv_ts - expected_end).total_seconds() / 3600

    log_console(f"\nOffset from expected:")
    log_console(f"  Start: {start_diff:+.1f} hours")
    log_console(f"  End: {end_diff:+.1f} hours")

    if abs(start_diff) > 1:
        log_console("\n✗ EXISTING CSV HAS WRONG TIMESTAMPS")
        log_console("   Data needs to be re-downloaded after fix is applied")
    else:
        log_console("\n✓ CSV timestamps are correct")
else:
    log_console("No CSV files found to test")

log_console("\n" + "=" * 60)
log_console("Summary:")
log_console("- Fixed _normalize_df_types() to NOT do timezone conversion")
log_console("- _normalize_ts_to_utc() handles all timezone conversions")
log_console("- Existing CSV files need to be re-downloaded to get correct timestamps")
log_console("=" * 60)
