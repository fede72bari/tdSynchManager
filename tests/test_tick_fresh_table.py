"""
Test TICK data on fresh/non-existent table to verify table creation handling
"""
from console_log import log_console

import asyncio
import pandas as pd
from src.tdSynchManager.ThetaDataV3Client import ThetaDataV3Client
from src.tdSynchManager.manager import ThetaSyncManager, install_td_server_error_logger
from src.tdSynchManager.config import ManagerConfig, Task, DiscoverPolicy
from src.tdSynchManager.credentials import get_influx_credentials
from influxdb_client_3 import InfluxDBClient3

# Get InfluxDB credentials
influx = get_influx_credentials()
influx_token = influx['token']
influx_url = influx.get('url', 'http://127.0.0.1:8181')
influx_bucket = influx.get('bucket', 'ThetaData')
symbols = ["TLRY"]

cfg = ManagerConfig(
    root_dir=r"tests/data",
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
    log_console("\n" + "="*80)
    log_console("TEST: TICK DATA ON FRESH/NON-EXISTENT TABLE")
    log_console("="*80)
    log_console(f"Testing symbols: {symbols}")
    log_console(f"Validation enabled: {cfg.enable_data_validation}")
    log_console(f"Strict mode: {cfg.validation_strict_mode}")
    log_console("")

    # First, check if table exists and drop it if present
    client = InfluxDBClient3(
        host=influx_url,
        database=influx_bucket,
        token=influx_token
    )

    try:
        measurement = "TLRY-option-tick"
        log_console(f"[SETUP] Checking if {measurement} exists...")
        result = client.query("SHOW TABLES")
        tables_df = result.to_pandas()
        table_exists = measurement in tables_df['table_name'].values

        if table_exists:
            log_console(f"[SETUP] Table {measurement} exists. Dropping it for fresh test...")
            # Note: InfluxDB v3 doesn't support DROP TABLE, so we'll just proceed
            log_console(f"[SETUP] NOTE: Cannot drop table in InfluxDB v3. Test will resume from existing data.")
        else:
            log_console(f"[SETUP] Table {measurement} does not exist. Perfect for testing table creation!")
    except Exception as e:
        log_console(f"[SETUP] Error checking table: {e}")
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

    log_console("\n[TEST] Starting download...")
    async with ThetaDataV3Client() as td_client:
        install_td_server_error_logger(td_client)
        manager = ThetaSyncManager(cfg, client=td_client)
        await manager.run([task])

    log_console("\n[TEST] Download completed.")

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
            log_console(f"\n[RESULT] ✅ Table {measurement} was created successfully!")

            # Query some data
            query = f'''
                SELECT time, symbol, close, volume
                FROM "{measurement}"
                ORDER BY time DESC
                LIMIT 10
            '''
            result = client.query(query)
            df = result.to_pandas()
            log_console(f"\n[RESULT] Sample data ({len(df)} rows):")
            log_console(df.to_string())
        else:
            log_console(f"\n[RESULT] ❌ Table {measurement} was NOT created!")
    except Exception as e:
        log_console(f"\n[RESULT] Error querying table: {e}")
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
            log_console(f"\n[LOGGER] Latest log file: {os.path.basename(latest_log)}")
            try:
                log_df = pd.read_parquet(latest_log)
                log_console(f"[LOGGER] Total log entries: {len(log_df)}")
                log_console("\n[LOGGER] Last 10 entries:")
                log_console(log_df.tail(10).to_string())
            except Exception as e:
                log_console(f"[LOGGER] Error reading log: {e}")
        else:
            log_console("\n[LOGGER] No log files found")
    else:
        log_console(f"\n[LOGGER] Log directory not found: {log_dir}")

    log_console("\n" + "="*80)
    log_console("TEST COMPLETED")
    log_console("="*80)


if __name__ == "__main__":
    asyncio.run(main())
