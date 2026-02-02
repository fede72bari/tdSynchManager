"""
Simple test to check timestamps in existing CSV files.
"""
from console_log import log_console
import sys
sys.path.insert(0, r'd:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src')

from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig
import pandas as pd
import os

log_console("=== Check Existing CSV Timestamps ===\n")

cfg = ManagerConfig(root_dir=r"tests/data", max_concurrency=1)
manager = ThetaSyncManager(cfg, client=None)

# List CSV files
csv_files = manager._list_series_files(asset="option", symbol="QQQ", interval="5m", sink_lower="csv")
log_console(f"Found {len(csv_files)} CSV files for QQQ options 5m")

if csv_files:
    # Check first file
    test_file = csv_files[0]
    log_console(f"\nAnalyzing: {os.path.basename(test_file)}")

    df = pd.read_csv(test_file)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    first_ts = df['timestamp'].min()
    last_ts = df['timestamp'].max()

    log_console(f"\nTimestamp range in file:")
    log_console(f"  First: {first_ts}")
    log_console(f"  Last:  {last_ts}")

    # Calculate time range
    first_hour = first_ts.hour
    last_hour = last_ts.hour

    log_console(f"\nHour range: {first_hour:02d}:XX to {last_hour:02d}:XX")

    # Check against expected ranges
    log_console(f"\nExpected ranges:")
    log_console(f"  If ET:  09:30-16:00 (market hours)")
    log_console(f"  If UTC: 14:30-21:00 (market hours in UTC)")

    # Determine what timezone these look like
    is_et_range = 9 <= first_hour <= 16
    is_utc_range = 14 <= first_hour <= 21

    log_console(f"\nAnalysis:")
    if is_utc_range and not is_et_range:
        log_console(f"  ✓ Timestamps appear to be in correct UTC range (14:30-21:00)")
    elif is_et_range and not is_utc_range:
        log_console(f"  ✗ Timestamps appear to be in ET range (NOT converted to UTC)")
    elif first_hour == 0 or first_hour == 4:
        offset_hours = first_hour + (24 - 14 if first_hour < 14 else 0)
        log_console(f"  ✗ Timestamps are WRONG - offset by ~{offset_hours} hours")
        log_console(f"     Current: starts at {first_hour:02d}:XX")
        log_console(f"     Expected: starts at 14:XX (UTC) or 09:XX (ET)")
        log_console(f"\n  NOTE: These are OLD files with the double-conversion bug.")
        log_console(f"        Re-download data to get correctly converted timestamps.")
    else:
        log_console(f"  ⚠ Timestamps don't match expected patterns")
        log_console(f"     Starting hour: {first_hour}")

    # Show sample rows
    log_console(f"\nFirst 3 rows:")
    log_console(df[['timestamp', 'symbol', 'strike', 'right', 'underlying_price']].head(3))

log_console("\n" + "=" * 70)
log_console("FIX SUMMARY:")
log_console("- Removed timezone conversion from _normalize_df_types()")
log_console("- Only _normalize_ts_to_utc() now handles ET->UTC conversion")
log_console("- Existing CSV files still have wrong timestamps (created before fix)")
log_console("- Solution: Re-download data to get correct timestamps")
log_console("=" * 70)
