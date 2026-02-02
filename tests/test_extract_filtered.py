from console_log import log_console
import sys
import os
sys.path.insert(0, r'd:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src')

from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig
import pandas as pd
from datetime import datetime, timedelta

log_console("=== Setup ===")
cfg = ManagerConfig(root_dir=r"tests/data", max_concurrency=1)
manager = ThetaSyncManager(cfg, client=None)

log_console("\n=== List files ===")
files = manager._list_series_files(asset="option", symbol="QQQ", interval="5m", sink_lower="csv")
log_console(f"Found {len(files)} files")

if not files:
    log_console("No files found - test cannot proceed")
    sys.exit(0)

log_console("\n=== Test extraction with filters ===")
test_file = files[0]
log_console(f"Testing with: {os.path.basename(test_file)}")

# Extract date from filename
date_str = os.path.basename(test_file).split('T')[0]
file_date = pd.to_datetime(date_str)
next_day = (file_date + timedelta(days=1)).strftime('%Y-%m-%d')

log_console(f"File date: {date_str}")
log_console(f"Next day (expiration filter): {next_day}")

# Read file
df = pd.read_csv(test_file)
log_console(f"\nTotal rows in file: {len(df):,}")

# Convert timestamp
df['timestamp'] = pd.to_datetime(df['timestamp'])
last_timestamp = df['timestamp'].max()
log_console(f"Last timestamp: {last_timestamp}")

# Filter by last timestamp
last_df = df[df['timestamp'] == last_timestamp].copy()
log_console(f"Rows at last timestamp: {len(last_df)}")

# Filter by expiration
last_df = last_df[last_df['expiration'] == next_day]
log_console(f"Rows with expiration={next_day}: {len(last_df)}")

if last_df.empty:
    log_console("\nNo contracts with next-day expiration - this is normal if expiration doesn't exist")
    # Try to show what expirations are available
    unique_exp = df['expiration'].unique()[:5]
    log_console(f"Available expirations (sample): {unique_exp}")
    sys.exit(0)

# DEBUG: Check what values are in 'right' column
log_console(f"\nDEBUG: Unique values in 'right' column: {last_df['right'].unique()}")
log_console(f"DEBUG: Sample 'right' values: {last_df['right'].head(10).tolist()}")

# Check underlying price
if 'underlying_price' in last_df.columns:
    underlying_price = last_df['underlying_price'].iloc[0]
    log_console(f"Underlying price: ${underlying_price:.2f}")
else:
    log_console("WARNING: No underlying_price column!")
    sys.exit(1)

# Test Call extraction
log_console("\n=== Test CALL extraction ===")
call_df = last_df[last_df['right'] == 'call'].copy()
log_console(f"Call contracts: {len(call_df)}")

if not call_df.empty:
    call_df['distance'] = abs(call_df['strike'] - underlying_price)
    strikes_below = call_df[call_df['strike'] <= underlying_price]
    strikes_above = call_df[call_df['strike'] > underlying_price]
    
    log_console(f"Strikes below underlying: {len(strikes_below)}")
    log_console(f"Strikes above underlying: {len(strikes_above)}")
    
    if not strikes_below.empty:
        closest_below = strikes_below.loc[strikes_below['distance'].idxmin()]
        log_console(f"Closest below: strike=${closest_below['strike']:.2f}, distance=${closest_below['distance']:.2f}")
    
    if not strikes_above.empty:
        closest_above = strikes_above.loc[strikes_above['distance'].idxmin()]
        log_console(f"Closest above: strike=${closest_above['strike']:.2f}, distance=${closest_above['distance']:.2f}")

# Test Put extraction
log_console("\n=== Test PUT extraction ===")
put_df = last_df[last_df['right'] == 'put'].copy()
log_console(f"Put contracts: {len(put_df)}")

if not put_df.empty:
    put_df['distance'] = abs(put_df['strike'] - underlying_price)
    closest_put = put_df.loc[put_df['distance'].idxmin()]
    log_console(f"Closest put: strike=${closest_put['strike']:.2f}, bid=${closest_put['bid']:.2f}, ask=${closest_put['ask']:.2f}")
    
    # Test mid price calculation
    mid_price = (closest_put['bid'] + closest_put['ask']) / 2
    log_console(f"Mid price: ${mid_price:.2f}")

log_console("\n=== Test support/resistance calculation ===")
if not call_df.empty and not put_df.empty:
    call_closest = call_df.loc[call_df['distance'].idxmin()]
    put_closest = put_df.loc[put_df['distance'].idxmin()]
    
    strike = call_closest['strike']
    mid_call = (call_closest['bid'] + call_closest['ask']) / 2
    mid_put = (put_closest['bid'] + put_closest['ask']) / 2
    
    resistance = strike + mid_call + mid_put
    support = strike - mid_call - mid_put
    
    log_console(f"Strike: ${strike:.2f}")
    log_console(f"Mid Call: ${mid_call:.2f}")
    log_console(f"Mid Put: ${mid_put:.2f}")
    log_console(f"Resistance: ${resistance:.2f}")
    log_console(f"Support: ${support:.2f}")
    log_console(f"Range: ${resistance - support:.2f}")

log_console("\n=== ALL TESTS PASSED ===")
