"""
Diagnostic script to verify which manager.py source is being loaded
and whether the timezone fix is present in the loaded code.
"""

import sys
from pathlib import Path

# Same setup as user's Jupyter notebook
PKG_SRC = r"D:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src"
sys.path.insert(0, PKG_SRC)

import inspect
import tdSynchManager.manager as manager_module
from tdSynchManager import ThetaSyncManager

print("=" * 80)
print("DIAGNOSTIC: Check which code is being loaded")
print("=" * 80)

# 1. Check which file is loaded
source_file = inspect.getsourcefile(ThetaSyncManager)
print(f"\n1. Loaded ThetaSyncManager from:")
print(f"   {source_file}")

expected_file = Path(PKG_SRC) / "tdSynchManager" / "manager.py"
print(f"\n2. Expected file:")
print(f"   {expected_file}")
print(f"   Match: {Path(source_file) == expected_file}")

# 3. Check if timezone fix is present in the loaded code
print("\n3. Checking for timezone fix in loaded code...")
try:
    source_code = inspect.getsource(ThetaSyncManager._download_and_store_options)

    # Search for the critical fix
    if 'tz_localize("America/New_York")' in source_code:
        print("   [OK] Timezone fix IS present in loaded code")

        # Count occurrences
        count = source_code.count('tz_localize("America/New_York")')
        print(f"   Found {count} occurrence(s) of tz_localize")
    else:
        print("   [FAIL] Timezone fix NOT present - using OLD code!")

    # 4. Extract the specific lines around effective_date_oi assignment
    print("\n4. Lines containing 'effective_date_oi' assignment:")
    lines = source_code.split('\n')
    for i, line in enumerate(lines):
        if 'effective_date_oi' in line and ('doi[' in line or 'doi_norm[' in line):
            print(f"   Line {i}: {line.strip()}")

except Exception as e:
    print(f"   [ERROR] Error getting source: {e}")

# 5. Check file modification time
print("\n5. File modification time:")
try:
    import os
    mtime = os.path.getmtime(source_file)
    from datetime import datetime
    mod_time = datetime.fromtimestamp(mtime)
    print(f"   Last modified: {mod_time}")
except Exception as e:
    print(f"   Error: {e}")

# 6. Read the actual file and check for fix
print("\n6. Checking actual file content on disk...")
try:
    with open(expected_file, 'r', encoding='utf-8') as f:
        file_content = f.read()

    if 'tz_localize("America/New_York")' in file_content:
        count = file_content.count('tz_localize("America/New_York")')
        print(f"   [OK] File on disk HAS timezone fix ({count} occurrences)")
    else:
        print(f"   [FAIL] File on disk MISSING timezone fix")

except Exception as e:
    print(f"   Error reading file: {e}")

print("\n" + "=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)
