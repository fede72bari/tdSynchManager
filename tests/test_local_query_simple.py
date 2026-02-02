"""Simple test for LOCAL DB QUERY functions without async client."""
from console_log import log_console
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
    
    log_console("=" * 70)
    log_console("TEST: list_available_data()")
    log_console("=" * 70)
    
    # List all available data
    try:
        available = manager.list_available_data()
        log_console(f"\nFound {len(available)} data series")
        
        if not available.empty:
            log_console("\nAvailable data:")
            log_console(available.to_string())
            
            log_console(f"\nColumns: {list(available.columns)}")
            
            # Show first entry details
            log_console(f"\nFirst entry:")
            first = available.iloc[0]
            for col in available.columns:
                log_console(f"  {col}: {first[col]}")
            
            return available
        else:
            log_console("\nNo data found in ./data directory")
            log_console("This is normal if you haven't synced any data yet.")
            return available
            
    except Exception as e:
        log_console(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_query_data(available):
    """Test query_local_data() function."""
    
    if available is None or available.empty:
        log_console("\nSkipping query test - no data available")
        return
    
    # Initialize manager with mock client
    cfg = ManagerConfig(root_dir="./", max_concurrency=5)
    manager = ThetaSyncManager(cfg, MockClient())
    
    log_console("\n" + "=" * 70)
    log_console("TEST: query_local_data()")
    log_console("=" * 70)
    
    # Use first available series
    first = available.iloc[0]
    asset = first['asset']
    symbol = first['symbol']
    interval = first['interval']
    sink = first['sink']
    
    log_console(f"\nQuerying: {asset}/{symbol}/{interval} from {sink}")
    
    try:
        # Get start date
        first_dt = first['first_datetime']
        if first_dt and isinstance(first_dt, str):
            start_date = first_dt[:10]  # YYYY-MM-DD
        else:
            start_date = "2024-01-01"  # fallback
        
        log_console(f"  Start date: {start_date}")
        log_console(f"  Max rows: 10")
        
        df, warnings = manager.query_local_data(
            asset=asset,
            symbol=symbol,
            interval=interval,
            sink=sink,
            start_date=start_date,
            max_rows=10
        )
        
        if warnings:
            log_console(f"\nWarnings:")
            for w in warnings:
                log_console(f"  - {w}")
        
        if df is not None:
            log_console(f"\nSuccess! Retrieved {len(df)} rows")
            log_console(f"Columns: {list(df.columns)}")
            log_console(f"\nFirst 5 rows:")
            log_console(df.head().to_string())
        else:
            log_console("\nNo data returned (check warnings above)")
            
    except Exception as e:
        log_console(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    log_console("Testing LOCAL DB QUERY functions\n")
    
    # Test 1: List available data
    available = test_list_available()
    
    # Test 2: Query local data
    if available is not None:
        test_query_data(available)
    
    log_console("\n" + "=" * 70)
    log_console("Tests completed")
    log_console("=" * 70)
