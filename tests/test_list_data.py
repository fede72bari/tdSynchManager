"""Quick test for list_available_data()"""
from console_log import log_console
import sys
sys.path.insert(0, 'src')

# Minimal imports
import os
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'

# Mock client
class MockClient:
    pass

from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig

# Config matching user's setup
cfg = ManagerConfig(root_dir=r"tests/data")
manager = ThetaSyncManager(cfg, MockClient())

log_console("=" * 70)
log_console("Testing list_available_data()")
log_console("=" * 70)

# Test 1: List ALL data
log_console("\n1. List ALL available data:")
try:
    all_data = manager.list_available_data()
    log_console(f"   Found {len(all_data)} series")
    if not all_data.empty:
        log_console(all_data.to_string())
    else:
        log_console("   No data found!")
except Exception as e:
    log_console(f"   ERROR: {e}")
    import traceback
    traceback.print_exc()

# Test 2: List only option data
log_console("\n2. List only OPTION data:")
try:
    option_data = manager.list_available_data(asset="option")
    log_console(f"   Found {len(option_data)} option series")
    if not option_data.empty:
        log_console(option_data.to_string())
except Exception as e:
    log_console(f"   ERROR: {e}")

# Test 3: List TLRY data
log_console("\n3. List TLRY data:")
try:
    tlry_data = manager.list_available_data(symbol="TLRY")
    log_console(f"   Found {len(tlry_data)} TLRY series")
    if not tlry_data.empty:
        log_console(tlry_data.to_string())
except Exception as e:
    log_console(f"   ERROR: {e}")

log_console("\n" + "=" * 70)
