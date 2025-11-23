"""
Test TICK data on fresh/non-existent table to verify table creation handling
"""

import asyncio
import pandas as pd
from src.tdSynchManager.client import ThetaDataV3Client
from src.tdSynchManager.manager import ThetaSyncManager, install_td_server_error_logger
from src.tdSynchManager.config import ManagerConfig, Task, DiscoverPolicy
from influxdb_client_3 import InfluxDBClient3

# Configuration
influx_url = "http://127.0.0.1:8181"
influx_bucket = "ThetaData"
influx_token = 'apiv3_reUhe6AEm4FjG4PHtLEW5wbt8MVUtiRtHPgm3Qw487pJFpVj6DlPTRxR1tvcW8bkY1IPM_PQEzHn5b1DVwZc2w'
symbols = ["TLRY"]

cfg = ManagerConfig(
    root_dir=r"C:\Users\Federico\Downloads",
    max_concurrency=80,
    max_file_mb=16,
    overlap_seconds=60,
    influx_url=influx_url,
    influx_bucket=influx_bucket,
    influx_token=influx_token,
    influx_org=None,
    influx_precision="nanosecond",
    influx_measure_prefix="",
    influx_write_batch=5000,

    # Enable validation with strict mode
    enable_data_validation=True,
    validation_strict_mode=True,
)


async def main():
    """Test tick data on fresh table"""
    print("\n" + "="*80)
    print("TEST: TICK DATA ON FRESH/NON-EXISTENT TABLE")
    print("="*80)
    print(f"Testing symbols: {symbols}")
    print(f"Validation enabled: {cfg.enable_data_validation}")
    print(f"Strict mode: {cfg.validation_strict_mode}")
    print("")

    # First, check if table exists and drop it if present
    client = InfluxDBClient3(
        host=influx_url,
        database=influx_bucket,
        token=influx_token
    )

    try:
        measurement = "TLRY-option-tick"
        print(f"[SETUP] Checking if {measurement} exists...")
        result = client.query("SHOW TABLES")
        tables_df = result.to_pandas()
        table_exists = measurement in tables_df['table_name'].values

        if table_exists:
            print(f"[SETUP] Table {measurement} exists. Dropping it for fresh test...")
            # Note: InfluxDB v3 doesn't support DROP TABLE, so we'll just proceed
            print(f"[SETUP] NOTE: Cannot drop table in InfluxDB v3. Test will resume from existing data.")
        else:
            print(f"[SETUP] Table {measurement} does not exist. Perfect for testing table creation!")
    except Exception as e:
        print(f"[SETUP] Error checking table: {e}")
    finally:
        client.close()

    # Run the task
    task = Task(
        asset="option",
        symbols=symbols,
        intervals=["tick"],
        sink="influxdb",
        enrich_bar_greeks=True,
        enrich_tick_greeks=True,
        first_date_override="20250821",
        ignore_existing=False,
        discover_policy=DiscoverPolicy(mode="skip")
    )

    print("\n[TEST] Starting download...")
    async with ThetaDataV3Client() as td_client:
        install_td_server_error_logger(td_client)
        manager = ThetaSyncManager(cfg, client=td_client)
        await manager.run([task])

    print("\n[TEST] Download completed.")

    # Check if table was created
    client = InfluxDBClient3(
        host=influx_url,
        database=influx_bucket,
        token=influx_token
    )

    try:
        measurement = "TLRY-option-tick"
        result = client.query("SHOW TABLES")
        tables_df = result.to_pandas()
        table_exists = measurement in tables_df['table_name'].values

        if table_exists:
            print(f"\n[RESULT] ✅ Table {measurement} was created successfully!")

            # Query some data
            query = f'''
                SELECT time, symbol, close, volume
                FROM "{measurement}"
                ORDER BY time DESC
                LIMIT 10
            '''
            result = client.query(query)
            df = result.to_pandas()
            print(f"\n[RESULT] Sample data ({len(df)} rows):")
            print(df.to_string())
        else:
            print(f"\n[RESULT] ❌ Table {measurement} was NOT created!")
    except Exception as e:
        print(f"\n[RESULT] Error querying table: {e}")
    finally:
        client.close()

    # Check logger files
    import os
    import glob
    log_dir = os.path.join(cfg.root_dir, "logs")
    if os.path.exists(log_dir):
        log_files = glob.glob(os.path.join(log_dir, "data_consistency_*.parquet"))
        if log_files:
            latest_log = max(log_files, key=os.path.getmtime)
            print(f"\n[LOGGER] Latest log file: {os.path.basename(latest_log)}")
            try:
                log_df = pd.read_parquet(latest_log)
                print(f"[LOGGER] Total log entries: {len(log_df)}")
                print("\n[LOGGER] Last 10 entries:")
                print(log_df.tail(10).to_string())
            except Exception as e:
                print(f"[LOGGER] Error reading log: {e}")
        else:
            print("\n[LOGGER] No log files found")
    else:
        print(f"\n[LOGGER] Log directory not found: {log_dir}")

    print("\n" + "="*80)
    print("TEST COMPLETED")
    print("="*80)


if __name__ == "__main__":
    asyncio.run(main())
