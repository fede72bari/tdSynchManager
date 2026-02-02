from console_log import log_console
import sys
import os
sys.path.insert(0, r'd:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src')

from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig
import pandas as pd
from datetime import timedelta

log_console("=== Setup ===")
cfg = ManagerConfig(root_dir=r"tests/data", max_concurrency=1)
manager = ThetaSyncManager(cfg, client=None)

log_console("\n=== List files ===")
files = manager._list_series_files(asset="option", symbol="QQQ", interval="5m", sink_lower="csv")
log_console(f"Found {len(files)} files")

# Group files by date
files_by_date = {}
for file_path in files:
    filename = os.path.basename(file_path)
    date_str = filename.split('T')[0]
    if date_str not in files_by_date:
        files_by_date[date_str] = []
    files_by_date[date_str].append(file_path)

log_console(f"Found {len(files_by_date)} unique days")

# Test extraction for first day
first_date = sorted(files_by_date.keys())[0]
log_console(f"\n=== Test extraction for {first_date} ===")

file_date = pd.to_datetime(first_date)
next_day = (file_date + timedelta(days=1)).strftime('%Y-%m-%d')
log_console(f"Next day (1DTE): {next_day}")

# Read ALL files for this day
day_dfs = []
for file_path in files_by_date[first_date]:
    df = pd.read_csv(file_path)
    if not df.empty:
        day_dfs.append(df)

log_console(f"Files for this day: {len(day_dfs)}")

# Concatenate
full_day_df = pd.concat(day_dfs, ignore_index=True)
full_day_df['timestamp'] = pd.to_datetime(full_day_df['timestamp'])
log_console(f"Total rows for day: {len(full_day_df):,}")

# Get last timestamp
last_timestamp = full_day_df['timestamp'].max()
log_console(f"Last timestamp: {last_timestamp}")

last_df = full_day_df[full_day_df['timestamp'] == last_timestamp].copy()
log_console(f"Rows at last timestamp: {len(last_df):,}")

# Filter 1DTE
last_df = last_df[last_df['expiration'] == next_day]
log_console(f"Rows with 1DTE (exp={next_day}): {len(last_df)}")

if not last_df.empty:
    underlying_price = last_df['underlying_price'].iloc[0]
    log_console(f"Underlying price: ${underlying_price:.2f}")

    # Extract call and put
    for right in ['call', 'put']:
        right_df = last_df[last_df['right'] == right].copy()
        log_console(f"\n{right.upper()} contracts: {len(right_df)}")

        if not right_df.empty:
            right_df['distance'] = abs(right_df['strike'] - underlying_price)

            strikes_below = right_df[right_df['strike'] <= underlying_price]
            strikes_above = right_df[right_df['strike'] > underlying_price]

            log_console(f"  Strikes below: {len(strikes_below)}")
            log_console(f"  Strikes above: {len(strikes_above)}")

            # Pick closest
            closest_row = None
            if not strikes_below.empty and not strikes_above.empty:
                closest_below = strikes_below.loc[strikes_below['distance'].idxmin()]
                closest_above = strikes_above.loc[strikes_above['distance'].idxmin()]
                if closest_below['distance'] <= closest_above['distance']:
                    closest_row = closest_below
                    log_console(f"  Selected: below @ ${closest_below['strike']:.2f} (distance ${closest_below['distance']:.2f})")
                else:
                    closest_row = closest_above
                    log_console(f"  Selected: above @ ${closest_above['strike']:.2f} (distance ${closest_above['distance']:.2f})")
            elif not strikes_below.empty:
                closest_row = strikes_below.loc[strikes_below['distance'].idxmin()]
                log_console(f"  Selected: below @ ${closest_row['strike']:.2f}")
            elif not strikes_above.empty:
                closest_row = strikes_above.loc[strikes_above['distance'].idxmin()]
                log_console(f"  Selected: above @ ${closest_row['strike']:.2f}")

            if closest_row is not None:
                mid_price = (closest_row['bid'] + closest_row['ask']) / 2
                log_console(f"  Bid: ${closest_row['bid']:.2f}, Ask: ${closest_row['ask']:.2f}, Mid: ${mid_price:.2f}")

log_console("\n=== TEST PASSED ===")
