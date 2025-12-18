"""Quick test for list_available_data()"""
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

print("=" * 70)
print("Testing list_available_data()")
print("=" * 70)

# Test 1: List ALL data
print("\n1. List ALL available data:")
try:
    all_data = manager.list_available_data()
    print(f"   Found {len(all_data)} series")
    if not all_data.empty:
        print(all_data.to_string())
    else:
        print("   No data found!")
except Exception as e:
    print(f"   ERROR: {e}")
    import traceback
    traceback.print_exc()

# Test 2: List only option data
print("\n2. List only OPTION data:")
try:
    option_data = manager.list_available_data(asset="option")
    print(f"   Found {len(option_data)} option series")
    if not option_data.empty:
        print(option_data.to_string())
except Exception as e:
    print(f"   ERROR: {e}")

# Test 3: List TLRY data
print("\n3. List TLRY data:")
try:
    tlry_data = manager.list_available_data(symbol="TLRY")
    print(f"   Found {len(tlry_data)} TLRY series")
    if not tlry_data.empty:
        print(tlry_data.to_string())
except Exception as e:
    print(f"   ERROR: {e}")

print("\n" + "=" * 70)
