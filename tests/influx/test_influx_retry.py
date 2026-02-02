"""
Test del sistema di retry e recovery per InfluxDB writes
"""
from console_log import log_console

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import time
from influxdb_client_3 import InfluxDBClient3
from tdSynchManager.influx_retry import InfluxWriteRetry, recover_failed_batches

def test_retry_logic():
    """Test del retry logic con batch simulati."""
    from tdSynchManager.credentials import get_influx_credentials

    log_console("=" * 80)
    log_console("TEST: InfluxDB Retry Logic")
    log_console("=" * 80)

    # Get InfluxDB credentials
    influx = get_influx_credentials()
    influx_token = influx['token']
    influx_url = influx.get('url', 'http://127.0.0.1:8181')
    influx_bucket = influx.get('bucket', 'ThetaData')

    # Crea retry manager
    retry_mgr = InfluxWriteRetry(
        max_retries=3,
        base_delay=2.0,  # 2s, 4s, 8s
        failed_batch_dir="./failed_influx_batches"
    )

    # Connetti a InfluxDB
    client = InfluxDBClient3(
        host=influx_url,
        token=influx_token,
        database=influx_bucket
    )

    # Simula scrittura di 5 batch
    log_console("\n" + "-" * 80)
    log_console("Writing 5 batches...")
    log_console("-" * 80)

    now_ns = int(time.time() * 1e9)

    for batch_idx in range(1, 6):
        # Crea batch di test
        lines = []
        for i in range(100):
            lines.append(f"retry_test,batch={batch_idx} value={i}i {now_ns + batch_idx*1000 + i}")

        # Scrivi con retry
        metadata = {
            'symbol': 'TEST',
            'interval': '5m',
            'date': '2025-12-05'
        }

        log_console(f"\n[BATCH {batch_idx}] Writing {len(lines)} points...")
        success = retry_mgr.write_with_retry(
            client=client,
            lines=lines,
            measurement="retry_test",
            batch_idx=batch_idx,
            metadata=metadata
        )

        if success:
            log_console(f"[BATCH {batch_idx}] ✓ SUCCESS")
        else:
            log_console(f"[BATCH {batch_idx}] ✗ FAILED after retries - saved for recovery")

    # Mostra statistiche
    retry_mgr.print_summary()

    # Test recovery
    log_console("\n\n" + "=" * 80)
    log_console("TEST: Failed Batch Recovery")
    log_console("=" * 80)

    failed_dir = "./failed_influx_batches"
    if os.path.exists(failed_dir) and os.listdir(failed_dir):
        log_console("\n[RECOVERY] Attempting to recover failed batches...")
        recover_failed_batches(
            failed_batch_dir=failed_dir,
            client=client,
            dry_run=False  # Cambia a True per solo simulazione
        )
    else:
        log_console("[RECOVERY] No failed batches to recover")

    log_console("\n" + "=" * 80)
    log_console("TEST COMPLETED")
    log_console("=" * 80)


if __name__ == "__main__":
    test_retry_logic()
