"""Test script for new LOCAL DB QUERY functions."""
from console_log import log_console
import asyncio
import sys
sys.path.insert(0, 'src')

from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client


async def test_list_and_query():
    """Test list_available_data() and query_local_data() functions."""
    
    # Initialize manager
    cfg = ManagerConfig(root_dir="./data", max_concurrency=5)
    
    async with ThetaDataV3Client(base_url="http://localhost:25503/v3") as client:
        manager = ThetaSyncManager(cfg, client)
        
        log_console("=" * 70)
        log_console("TEST 1: list_available_data()")
        log_console("=" * 70)
        
        # List all available data
        try:
            available = manager.list_available_data()
            log_console(f"\nFound {len(available)} data series:")
            log_console(available.to_string())
            
            if not available.empty:
                log_console(f"\nColumns: {list(available.columns)}")
                log_console(f"\nFirst entry:")
                first = available.iloc[0]
                log_console(f"  Asset: {first['asset']}")
                log_console(f"  Symbol: {first['symbol']}")
                log_console(f"  Interval: {first['interval']}")
                log_console(f"  Sink: {first['sink']}")
                log_console(f"  First datetime: {first['first_datetime']}")
                log_console(f"  Last datetime: {first['last_datetime']}")
                log_console(f"  File count: {first['file_count']}")
                log_console(f"  Total size MB: {first['total_size_mb']}")
        except Exception as e:
            log_console(f"Error in list_available_data(): {e}")
            import traceback
            traceback.print_exc()
        
        log_console("\n" + "=" * 70)
        log_console("TEST 2: query_local_data()")
        log_console("=" * 70)
        
        # Try to query data if available
        if not available.empty:
            first = available.iloc[0]
            asset = first['asset']
            symbol = first['symbol']
            interval = first['interval']
            sink = first['sink']
            
            log_console(f"\nQuerying data for:")
            log_console(f"  Asset: {asset}, Symbol: {symbol}, Interval: {interval}, Sink: {sink}")
            
            try:
                # Extract start date from first_datetime
                first_dt = first['first_datetime']
                if first_dt:
                    # Parse datetime and get just the date
                    if isinstance(first_dt, str):
                        start_date = first_dt[:10]  # Get YYYY-MM-DD
                    else:
                        start_date = str(first_dt)[:10]
                    
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
                        log_console(f"\nWarnings: {warnings}")
                    
                    if df is not None:
                        log_console(f"\nQuery returned {len(df)} rows")
                        log_console(f"Columns: {list(df.columns)}")
                        log_console(f"\nFirst few rows:")
                        log_console(df.head().to_string())
                    else:
                        log_console("\nNo data returned")
                else:
                    log_console("\nCannot test query: no first_datetime available")
                    
            except Exception as e:
                log_console(f"Error in query_local_data(): {e}")
                import traceback
                traceback.print_exc()
        else:
            log_console("\nNo data available to query - skipping test")


if __name__ == "__main__":
    asyncio.run(test_list_and_query())
