"""Direct test without full imports"""
import sys
import os
import importlib.util

# Load manager module directly
spec = importlib.util.spec_from_file_location("manager", "src/tdSynchManager/manager.py")
manager_module = importlib.util.module_from_spec(spec)

# Mock the imports that manager needs
class MockConfig:
    root_dir = r"tests/data"
    cache_dir_name = ".cache"
    cache_file_name = "first_date_cache.json"
    max_concurrency = 5

sys.modules['tdSynchManager.config'] = type(sys)('tdSynchManager.config')
sys.modules['tdSynchManager.config'].ManagerConfig = MockConfig

class MockClient:
    pass

# Now load the module
spec.loader.exec_module(manager_module)

# Create manager instance
ThetaSyncManager = manager_module.ThetaSyncManager
manager = ThetaSyncManager(MockConfig(), MockClient())

print("=" * 70)
print("Testing list_available_data()")
print("=" * 70)

# Test
print("\nListing all available data...")
try:
    result = manager.list_available_data()
    print(f"Found {len(result)} series\n")
    if not result.empty:
        print(result.to_string())
    else:
        print("No data found - is the path correct?")
        print(f"Looking in: {MockConfig.root_dir}/data/")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
