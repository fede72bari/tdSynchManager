"""
Example usage of the new LOCAL DB QUERY functions.

These functions allow you to:
1. List all available data in your local sinks (CSV, Parquet, InfluxDB)
2. Query and extract data from local storage with filtering

Author: Claude Code
Date: 2025-11-14
"""

import asyncio
from tdSynchManager import ManagerConfig, ThetaSyncManager
from tdSynchManager.client import ThetaDataV3Client


async def example_usage():
    """Demonstrate how to use list_available_data() and query_local_data()."""
    
    # Initialize configuration and client
    cfg = ManagerConfig(root_dir="./data", max_concurrency=5)
    
    async with ThetaDataV3Client(base_url="http://localhost:25503/v3") as client:
        manager = ThetaSyncManager(cfg, client)
        
        # =====================================================================
        # EXAMPLE 1: List all available data
        # =====================================================================
        print("=" * 70)
        print("EXAMPLE 1: List all available data")
        print("=" * 70)
        
        available = manager.list_available_data()
        print(f"\nFound {len(available)} data series\n")
        print(available)
        
        # =====================================================================
        # EXAMPLE 2: List only stock data
        # =====================================================================
        print("\n" + "=" * 70)
        print("EXAMPLE 2: List only stock data")
        print("=" * 70)
        
        stocks = manager.list_available_data(asset="stock")
        print(f"\nFound {len(stocks)} stock series\n")
        print(stocks)
        
        # =====================================================================
        # EXAMPLE 3: List AAPL data in all formats
        # =====================================================================
        print("\n" + "=" * 70)
        print("EXAMPLE 3: List AAPL data in all formats")
        print("=" * 70)
        
        aapl = manager.list_available_data(symbol="AAPL")
        print(f"\nFound {len(aapl)} AAPL series\n")
        print(aapl)
        
        # =====================================================================
        # EXAMPLE 4: List all 5-minute interval data
        # =====================================================================
        print("\n" + "=" * 70)
        print("EXAMPLE 4: List all 5-minute interval data")
        print("=" * 70)
        
        five_min = manager.list_available_data(interval="5m")
        print(f"\nFound {len(five_min)} 5-minute series\n")
        print(five_min)
        
        # =====================================================================
        # EXAMPLE 5: Query specific data with date range
        # =====================================================================
        if not available.empty:
            print("\n" + "=" * 70)
            print("EXAMPLE 5: Query data with date range")
            print("=" * 70)
            
            # Use first available series as example
            first = available.iloc[0]
            
            print(f"\nQuerying {first['symbol']} {first['interval']} from {first['sink']}")
            
            # Query with just start date (gets all data from that date onwards)
            df, warnings = manager.query_local_data(
                asset=first['asset'],
                symbol=first['symbol'],
                interval=first['interval'],
                sink=first['sink'],
                start_date="2024-01-01",
                max_rows=100
            )
            
            if warnings:
                print(f"\nWarnings: {warnings}")
            
            if df is not None:
                print(f"\nRetrieved {len(df)} rows")
                print(f"Columns: {list(df.columns)}")
                print(f"\nFirst 5 rows:")
                print(df.head())
            
            # ================================================================= 
            # EXAMPLE 6: Query with specific datetime range
            # =================================================================
            print("\n" + "=" * 70)
            print("EXAMPLE 6: Query with specific datetime range")
            print("=" * 70)
            
            df2, warnings2 = manager.query_local_data(
                asset=first['asset'],
                symbol=first['symbol'],
                interval=first['interval'],
                sink=first['sink'],
                start_datetime="2024-01-01T09:30:00",
                end_datetime="2024-01-01T16:00:00",
                max_rows=50
            )
            
            if warnings2:
                print(f"\nWarnings: {warnings2}")
            
            if df2 is not None:
                print(f"\nRetrieved {len(df2)} rows between market hours")
                print(f"\nFirst 5 rows:")
                print(df2.head())
        
        else:
            print("\n" + "=" * 70)
            print("No data available - sync some data first!")
            print("=" * 70)


if __name__ == "__main__":
    asyncio.run(example_usage())
