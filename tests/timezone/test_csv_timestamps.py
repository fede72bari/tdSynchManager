"""
Simple test to check timestamps in existing CSV files.
"""
import sys
sys.path.insert(0, r'd:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src')

from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig
import pandas as pd
import os

print("=== Check Existing CSV Timestamps ===\n")

cfg = ManagerConfig(root_dir=r"C:\Users\Federico\Downloads", max_concurrency=1)
manager = ThetaSyncManager(cfg, client=None)

# List CSV files
csv_files = manager._list_series_files(asset="option", symbol="QQQ", interval="5m", sink_lower="csv")
print(f"Found {len(csv_files)} CSV files for QQQ options 5m")

if csv_files:
    # Check first file
    test_file = csv_files[0]
    print(f"\nAnalyzing: {os.path.basename(test_file)}")

    df = pd.read_csv(test_file)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    first_ts = df['timestamp'].min()
    last_ts = df['timestamp'].max()

    print(f"\nTimestamp range in file:")
    print(f"  First: {first_ts}")
    print(f"  Last:  {last_ts}")

    # Calculate time range
    first_hour = first_ts.hour
    last_hour = last_ts.hour

    print(f"\nHour range: {first_hour:02d}:XX to {last_hour:02d}:XX")

    # Check against expected ranges
    print(f"\nExpected ranges:")
    print(f"  If ET:  09:30-16:00 (market hours)")
    print(f"  If UTC: 14:30-21:00 (market hours in UTC)")

    # Determine what timezone these look like
    is_et_range = 9 <= first_hour <= 16
    is_utc_range = 14 <= first_hour <= 21

    print(f"\nAnalysis:")
    if is_utc_range and not is_et_range:
        print(f"  ✓ Timestamps appear to be in correct UTC range (14:30-21:00)")
    elif is_et_range and not is_utc_range:
        print(f"  ✗ Timestamps appear to be in ET range (NOT converted to UTC)")
    elif first_hour == 0 or first_hour == 4:
        offset_hours = first_hour + (24 - 14 if first_hour < 14 else 0)
        print(f"  ✗ Timestamps are WRONG - offset by ~{offset_hours} hours")
        print(f"     Current: starts at {first_hour:02d}:XX")
        print(f"     Expected: starts at 14:XX (UTC) or 09:XX (ET)")
        print(f"\n  NOTE: These are OLD files with the double-conversion bug.")
        print(f"        Re-download data to get correctly converted timestamps.")
    else:
        print(f"  ⚠ Timestamps don't match expected patterns")
        print(f"     Starting hour: {first_hour}")

    # Show sample rows
    print(f"\nFirst 3 rows:")
    print(df[['timestamp', 'symbol', 'strike', 'right', 'underlying_price']].head(3))

print("\n" + "=" * 70)
print("FIX SUMMARY:")
print("- Removed timezone conversion from _normalize_df_types()")
print("- Only _normalize_ts_to_utc() now handles ET->UTC conversion")
print("- Existing CSV files still have wrong timestamps (created before fix)")
print("- Solution: Re-download data to get correct timestamps")
print("=" * 70)
