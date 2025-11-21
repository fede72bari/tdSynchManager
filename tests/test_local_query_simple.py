"""Simple test for LOCAL DB QUERY functions without async client."""
import sys
import os
sys.path.insert(0, 'src')

# Mock client for testing
class MockClient:
    pass

# Import after path setup
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig


def test_list_available():
    """Test list_available_data() function."""
    
    # Initialize manager with mock client
    cfg = ManagerConfig(root_dir="./", max_concurrency=5)
    manager = ThetaSyncManager(cfg, MockClient())
    
    print("=" * 70)
    print("TEST: list_available_data()")
    print("=" * 70)
    
    # List all available data
    try:
        available = manager.list_available_data()
        print(f"\nFound {len(available)} data series")
        
        if not available.empty:
            print("\nAvailable data:")
            print(available.to_string())
            
            print(f"\nColumns: {list(available.columns)}")
            
            # Show first entry details
            print(f"\nFirst entry:")
            first = available.iloc[0]
            for col in available.columns:
                print(f"  {col}: {first[col]}")
            
            return available
        else:
            print("\nNo data found in ./data directory")
            print("This is normal if you haven't synced any data yet.")
            return available
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_query_data(available):
    """Test query_local_data() function."""
    
    if available is None or available.empty:
        print("\nSkipping query test - no data available")
        return
    
    # Initialize manager with mock client
    cfg = ManagerConfig(root_dir="./", max_concurrency=5)
    manager = ThetaSyncManager(cfg, MockClient())
    
    print("\n" + "=" * 70)
    print("TEST: query_local_data()")
    print("=" * 70)
    
    # Use first available series
    first = available.iloc[0]
    asset = first['asset']
    symbol = first['symbol']
    interval = first['interval']
    sink = first['sink']
    
    print(f"\nQuerying: {asset}/{symbol}/{interval} from {sink}")
    
    try:
        # Get start date
        first_dt = first['first_datetime']
        if first_dt and isinstance(first_dt, str):
            start_date = first_dt[:10]  # YYYY-MM-DD
        else:
            start_date = "2024-01-01"  # fallback
        
        print(f"  Start date: {start_date}")
        print(f"  Max rows: 10")
        
        df, warnings = manager.query_local_data(
            asset=asset,
            symbol=symbol,
            interval=interval,
            sink=sink,
            start_date=start_date,
            max_rows=10
        )
        
        if warnings:
            print(f"\nWarnings:")
            for w in warnings:
                print(f"  - {w}")
        
        if df is not None:
            print(f"\nSuccess! Retrieved {len(df)} rows")
            print(f"Columns: {list(df.columns)}")
            print(f"\nFirst 5 rows:")
            print(df.head().to_string())
        else:
            print("\nNo data returned (check warnings above)")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("Testing LOCAL DB QUERY functions\n")
    
    # Test 1: List available data
    available = test_list_available()
    
    # Test 2: Query local data
    if available is not None:
        test_query_data(available)
    
    print("\n" + "=" * 70)
    print("Tests completed")
    print("=" * 70)
