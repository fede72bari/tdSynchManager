"""
Test del sistema di retry e recovery per InfluxDB writes
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import time
from influxdb_client_3 import InfluxDBClient3
from tdSynchManager.influx_retry import InfluxWriteRetry, recover_failed_batches

def test_retry_logic():
    """Test del retry logic con batch simulati."""

    print("=" * 80)
    print("TEST: InfluxDB Retry Logic")
    print("=" * 80)

    token = 'apiv3_reUhe6AEm4FjG4PHtLEW5wbt8MVUtiRtHPgm3Qw487pJFpVj6DlPTRxR1tvcW8bkY1IPM_PQEzHn5b1DVwZc2w'

    # Crea retry manager
    retry_mgr = InfluxWriteRetry(
        max_retries=3,
        base_delay=2.0,  # 2s, 4s, 8s
        failed_batch_dir="./failed_influx_batches"
    )

    # Connetti a InfluxDB
    client = InfluxDBClient3(
        host="http://127.0.0.1:8181",
        token=token,
        database="ThetaData"
    )

    # Simula scrittura di 5 batch
    print("\n" + "-" * 80)
    print("Writing 5 batches...")
    print("-" * 80)

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

        print(f"\n[BATCH {batch_idx}] Writing {len(lines)} points...")
        success = retry_mgr.write_with_retry(
            client=client,
            lines=lines,
            measurement="retry_test",
            batch_idx=batch_idx,
            metadata=metadata
        )

        if success:
            print(f"[BATCH {batch_idx}] ✓ SUCCESS")
        else:
            print(f"[BATCH {batch_idx}] ✗ FAILED after retries - saved for recovery")

    # Mostra statistiche
    retry_mgr.print_summary()

    # Test recovery
    print("\n\n" + "=" * 80)
    print("TEST: Failed Batch Recovery")
    print("=" * 80)

    failed_dir = "./failed_influx_batches"
    if os.path.exists(failed_dir) and os.listdir(failed_dir):
        print("\n[RECOVERY] Attempting to recover failed batches...")
        recover_failed_batches(
            failed_batch_dir=failed_dir,
            client=client,
            dry_run=False  # Cambia a True per solo simulazione
        )
    else:
        print("[RECOVERY] No failed batches to recover")

    print("\n" + "=" * 80)
    print("TEST COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    test_retry_logic()
