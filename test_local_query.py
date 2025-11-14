"""Test script for new LOCAL DB QUERY functions."""
import asyncio
import sys
sys.path.insert(0, 'src')

from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig
from tdSynchManager.client import ThetaDataV3Client


async def test_list_and_query():
    """Test list_available_data() and query_local_data() functions."""
    
    # Initialize manager
    cfg = ManagerConfig(root_dir="./data", max_concurrency=5)
    
    async with ThetaDataV3Client(base_url="http://localhost:25503/v3") as client:
        manager = ThetaSyncManager(cfg, client)
        
        print("=" * 70)
        print("TEST 1: list_available_data()")
        print("=" * 70)
        
        # List all available data
        try:
            available = manager.list_available_data()
            print(f"\nFound {len(available)} data series:")
            print(available.to_string())
            
            if not available.empty:
                print(f"\nColumns: {list(available.columns)}")
                print(f"\nFirst entry:")
                first = available.iloc[0]
                print(f"  Asset: {first['asset']}")
                print(f"  Symbol: {first['symbol']}")
                print(f"  Interval: {first['interval']}")
                print(f"  Sink: {first['sink']}")
                print(f"  First datetime: {first['first_datetime']}")
                print(f"  Last datetime: {first['last_datetime']}")
                print(f"  File count: {first['file_count']}")
                print(f"  Total size MB: {first['total_size_mb']}")
        except Exception as e:
            print(f"Error in list_available_data(): {e}")
            import traceback
            traceback.print_exc()
        
        print("\n" + "=" * 70)
        print("TEST 2: query_local_data()")
        print("=" * 70)
        
        # Try to query data if available
        if not available.empty:
            first = available.iloc[0]
            asset = first['asset']
            symbol = first['symbol']
            interval = first['interval']
            sink = first['sink']
            
            print(f"\nQuerying data for:")
            print(f"  Asset: {asset}, Symbol: {symbol}, Interval: {interval}, Sink: {sink}")
            
            try:
                # Extract start date from first_datetime
                first_dt = first['first_datetime']
                if first_dt:
                    # Parse datetime and get just the date
                    if isinstance(first_dt, str):
                        start_date = first_dt[:10]  # Get YYYY-MM-DD
                    else:
                        start_date = str(first_dt)[:10]
                    
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
                        print(f"\nWarnings: {warnings}")
                    
                    if df is not None:
                        print(f"\nQuery returned {len(df)} rows")
                        print(f"Columns: {list(df.columns)}")
                        print(f"\nFirst few rows:")
                        print(df.head().to_string())
                    else:
                        print("\nNo data returned")
                else:
                    print("\nCannot test query: no first_datetime available")
                    
            except Exception as e:
                print(f"Error in query_local_data(): {e}")
                import traceback
                traceback.print_exc()
        else:
            print("\nNo data available to query - skipping test")


if __name__ == "__main__":
    asyncio.run(test_list_and_query())
