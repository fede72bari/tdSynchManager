"""
Example usage of the new LOCAL DB QUERY functions.

These functions allow you to:
1. List all available data in your local sinks (CSV, Parquet, InfluxDB)
2. Query and extract data from local storage with filtering

Author: Claude Code
Date: 2025-11-14
"""
from console_log import log_console

import asyncio
from tdSynchManager import ManagerConfig, ThetaSyncManager
from tdSynchManager.ThetaDataV3Client import ThetaDataV3Client


async def example_usage():
    """Demonstrate how to use list_available_data() and query_local_data()."""
    
    # Initialize configuration and client
    cfg = ManagerConfig(root_dir="./data", max_concurrency=5)
    
    async with ThetaDataV3Client(base_url="http://localhost:25503/v3") as client:
        manager = ThetaSyncManager(cfg, client)
        
        # =====================================================================
        # EXAMPLE 1: List all available data
        # =====================================================================
        log_console("=" * 70)
        log_console("EXAMPLE 1: List all available data")
        log_console("=" * 70)
        
        available = manager.list_available_data()
        log_console(f"\nFound {len(available)} data series\n")
        log_console(available)
        
        # =====================================================================
        # EXAMPLE 2: List only stock data
        # =====================================================================
        log_console("\n" + "=" * 70)
        log_console("EXAMPLE 2: List only stock data")
        log_console("=" * 70)
        
        stocks = manager.list_available_data(asset="stock")
        log_console(f"\nFound {len(stocks)} stock series\n")
        log_console(stocks)
        
        # =====================================================================
        # EXAMPLE 3: List AAPL data in all formats
        # =====================================================================
        log_console("\n" + "=" * 70)
        log_console("EXAMPLE 3: List AAPL data in all formats")
        log_console("=" * 70)
        
        aapl = manager.list_available_data(symbol="AAPL")
        log_console(f"\nFound {len(aapl)} AAPL series\n")
        log_console(aapl)
        
        # =====================================================================
        # EXAMPLE 4: List all 5-minute interval data
        # =====================================================================
        log_console("\n" + "=" * 70)
        log_console("EXAMPLE 4: List all 5-minute interval data")
        log_console("=" * 70)
        
        five_min = manager.list_available_data(interval="5m")
        log_console(f"\nFound {len(five_min)} 5-minute series\n")
        log_console(five_min)
        
        # =====================================================================
        # EXAMPLE 5: Query specific data with date range
        # =====================================================================
        if not available.empty:
            log_console("\n" + "=" * 70)
            log_console("EXAMPLE 5: Query data with date range")
            log_console("=" * 70)
            
            # Use first available series as example
            first = available.iloc[0]
            
            log_console(f"\nQuerying {first['symbol']} {first['interval']} from {first['sink']}")
            
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
                log_console(f"\nWarnings: {warnings}")
            
            if df is not None:
                log_console(f"\nRetrieved {len(df)} rows")
                log_console(f"Columns: {list(df.columns)}")
                log_console(f"\nFirst 5 rows:")
                log_console(df.head())
            
            # ================================================================= 
            # EXAMPLE 6: Query with specific datetime range
            # =================================================================
            log_console("\n" + "=" * 70)
            log_console("EXAMPLE 6: Query with specific datetime range")
            log_console("=" * 70)
            
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
                log_console(f"\nWarnings: {warnings2}")
            
            if df2 is not None:
                log_console(f"\nRetrieved {len(df2)} rows between market hours")
                log_console(f"\nFirst 5 rows:")
                log_console(df2.head())
        
        else:
            log_console("\n" + "=" * 70)
            log_console("No data available - sync some data first!")
            log_console("=" * 70)


if __name__ == "__main__":
    asyncio.run(example_usage())
