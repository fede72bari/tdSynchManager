"""
Diagnostic script to verify which manager.py source is being loaded
and whether the timezone fix is present in the loaded code.
"""
from console_log import log_console

import sys
from pathlib import Path

# Same setup as user's Jupyter notebook
PKG_SRC = r"D:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src"
sys.path.insert(0, PKG_SRC)

import inspect
import tdSynchManager.manager as manager_module
from tdSynchManager import ThetaSyncManager

log_console("=" * 80)
log_console("DIAGNOSTIC: Check which code is being loaded")
log_console("=" * 80)

# 1. Check which file is loaded
source_file = inspect.getsourcefile(ThetaSyncManager)
log_console(f"\n1. Loaded ThetaSyncManager from:")
log_console(f"   {source_file}")

expected_file = Path(PKG_SRC) / "tdSynchManager" / "manager.py"
log_console(f"\n2. Expected file:")
log_console(f"   {expected_file}")
log_console(f"   Match: {Path(source_file) == expected_file}")

# 3. Check if timezone fix is present in the loaded code
log_console("\n3. Checking for timezone fix in loaded code...")
try:
    source_code = inspect.getsource(ThetaSyncManager._download_and_store_options)

    # Search for the critical fix
    if 'tz_localize("America/New_York")' in source_code:
        log_console("   [OK] Timezone fix IS present in loaded code")

        # Count occurrences
        count = source_code.count('tz_localize("America/New_York")')
        log_console(f"   Found {count} occurrence(s) of tz_localize")
    else:
        log_console("   [FAIL] Timezone fix NOT present - using OLD code!")

    # 4. Extract the specific lines around effective_date_oi assignment
    log_console("\n4. Lines containing 'effective_date_oi' assignment:")
    lines = source_code.split('\n')
    for i, line in enumerate(lines):
        if 'effective_date_oi' in line and ('doi[' in line or 'doi_norm[' in line):
            log_console(f"   Line {i}: {line.strip()}")

except Exception as e:
    log_console(f"   [ERROR] Error getting source: {e}")

# 5. Check file modification time
log_console("\n5. File modification time:")
try:
    import os
    mtime = os.path.getmtime(source_file)
    from datetime import datetime
    mod_time = datetime.fromtimestamp(mtime)
    log_console(f"   Last modified: {mod_time}")
except Exception as e:
    log_console(f"   Error: {e}")

# 6. Read the actual file and check for fix
log_console("\n6. Checking actual file content on disk...")
try:
    with open(expected_file, 'r', encoding='utf-8') as f:
        file_content = f.read()

    if 'tz_localize("America/New_York")' in file_content:
        count = file_content.count('tz_localize("America/New_York")')
        log_console(f"   [OK] File on disk HAS timezone fix ({count} occurrences)")
    else:
        log_console(f"   [FAIL] File on disk MISSING timezone fix")

except Exception as e:
    log_console(f"   Error reading file: {e}")

log_console("\n" + "=" * 80)
log_console("DIAGNOSTIC COMPLETE")
log_console("=" * 80)
