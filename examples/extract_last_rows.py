from console_log import log_console
import sys
import os
import pandas as pd
from pathlib import Path

sys.path.insert(0, r'd:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src')

from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig

# Create minimal config
cfg = ManagerConfig(
    root_dir="examples/data",
    max_concurrency=1
)

# Create manager instance (no client needed for reading files)
manager = ThetaSyncManager(cfg, client=None)

# Use manager's internal method to list files
files = manager._list_series_files(
    asset="option",
    symbol="QQQ",
    interval="5m",
    sink="csv"
)

log_console(f"Found {len(files)} files for QQQ options 5m CSV")

# Extract last row from each file
last_rows = []

for file_path in sorted(files):
    try:
        # Extract date from filename
        filename = os.path.basename(file_path)
        # Format: YYYY-MM-DDTHH-MM-SSZ-QQQ-option-5m.csv
        date_str = filename.split('T')[0]
        
        # Read only the last row (efficient)
        df = pd.read_csv(file_path)
        
        if not df.empty:
            last_row = df.iloc[-1].copy()
            last_row['file_date'] = date_str
            last_row['file_path'] = file_path
            last_rows.append(last_row)
            log_console(f"  {date_str}: {len(df)} rows, last timestamp: {last_row.get('timestamp', 'N/A')}")
        else:
            log_console(f"  {date_str}: EMPTY FILE")
            
    except Exception as e:
        log_console(f"  Error reading {file_path}: {e}")

# Create final DataFrame
if last_rows:
    result_df = pd.DataFrame(last_rows)
    
    log_console(f"\n=== SUMMARY ===")
    log_console(f"Total days processed: {len(result_df)}")
    log_console(f"Date range: {result_df['file_date'].min()} to {result_df['file_date'].max()}")
    
    # Save to CSV
    output_file = "examples/data/qqq_5m_last_rows.csv"
    result_df.to_csv(output_file, index=False)
    log_console(f"\nSaved to: {output_file}")
    
    # Show first few rows
    log_console(f"\nFirst 5 rows:")
    log_console(result_df.head())
    
    log_console(f"\nColumns: {list(result_df.columns)}")
else:
    log_console("\nNo data found!")
