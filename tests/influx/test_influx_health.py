"""
Test InfluxDB health e saturazione prima di scrivere
"""
from console_log import log_console

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import time
import psutil
from influxdb_client_3 import InfluxDBClient3

def check_influx_health(host="http://127.0.0.1:8181", token="your_token", database="ThetaData"):
    """Verifica salute e saturazione InfluxDB."""

    log_console("=" * 80)
    log_console("InfluxDB Health Check")
    log_console("=" * 80)

    try:
        client = InfluxDBClient3(host=host, token=token, database=database)

        # Test 1: Query semplice per verificare connessione
        log_console("\n[CHECK 1] Connessione e autenticazione...")
        t0 = time.time()
        try:
            # Query vuota veloce
            result = client.query("SELECT COUNT(*) FROM 'dummy-table-that-does-not-exist' LIMIT 1")
            elapsed = time.time() - t0
            log_console(f"[CHECK 1] ✓ Connessione OK (risposta in {elapsed:.3f}s)")
        except Exception as e:
            elapsed = time.time() - t0
            # Se è solo "table not found" va bene
            if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                log_console(f"[CHECK 1] ✓ Connessione OK (risposta in {elapsed:.3f}s)")
            else:
                log_console(f"[CHECK 1] ✗ ERRORE: {e}")
                return False

        # Test 2: Misura latenza query
        log_console("\n[CHECK 2] Latenza query...")
        t0 = time.time()
        try:
            # Query su una tabella esistente (se c'è)
            result = client.query("SHOW TABLES")
            elapsed = time.time() - t0
            log_console(f"[CHECK 2] Latenza query: {elapsed:.3f}s")

            if elapsed > 5.0:
                log_console(f"[CHECK 2] ⚠ WARNING: Latenza alta ({elapsed:.1f}s) - InfluxDB potrebbe essere saturo")
                return False
            elif elapsed > 2.0:
                log_console(f"[CHECK 2] ⚠ ATTENZIONE: Latenza moderata ({elapsed:.1f}s)")
            else:
                log_console(f"[CHECK 2] ✓ Latenza OK")
        except Exception as e:
            log_console(f"[CHECK 2] ✗ Query fallita: {e}")
            return False

        # Test 3: Test write piccolo
        log_console("\n[CHECK 3] Test write (100 punti)...")
        t0 = time.time()
        try:
            # Scrivi 100 punti di test
            lines = []
            now_ns = int(time.time() * 1e9)
            for i in range(100):
                lines.append(f"health_check,test=ping value={i}i {now_ns + i}")

            client.write(record=lines)
            elapsed = time.time() - t0
            log_console(f"[CHECK 3] Write test: {elapsed:.3f}s")

            if elapsed > 3.0:
                log_console(f"[CHECK 3] ⚠ WARNING: Write lento ({elapsed:.1f}s) - InfluxDB potrebbe essere saturo")
                return False
            else:
                log_console(f"[CHECK 3] ✓ Write OK")
        except Exception as e:
            elapsed = time.time() - t0
            log_console(f"[CHECK 3] ✗ Write fallito dopo {elapsed:.3f}s: {e}")
            return False

        # Test 4: Check sistema (se siamo in locale)
        log_console("\n[CHECK 4] Risorse sistema locale...")
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            mem = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            log_console(f"[CHECK 4] CPU: {cpu_percent:.1f}%")
            log_console(f"[CHECK 4] RAM: {mem.percent:.1f}% usata ({mem.used/1e9:.1f}GB / {mem.total/1e9:.1f}GB)")
            log_console(f"[CHECK 4] Disk: {disk.percent:.1f}% usato ({disk.used/1e9:.1f}GB / {disk.total/1e9:.1f}GB)")

            warnings = []
            if cpu_percent > 90:
                warnings.append(f"CPU alta ({cpu_percent:.1f}%)")
            if mem.percent > 90:
                warnings.append(f"RAM alta ({mem.percent:.1f}%)")
            if disk.percent > 90:
                warnings.append(f"Disk pieno ({disk.percent:.1f}%)")

            if warnings:
                log_console(f"[CHECK 4] ⚠ WARNING: {', '.join(warnings)}")
                return False
            else:
                log_console(f"[CHECK 4] ✓ Risorse sistema OK")
        except Exception as e:
            log_console(f"[CHECK 4] (skip - non locale o errore: {e})")

        log_console("\n" + "=" * 80)
        log_console("✓ HEALTH CHECK PASSED - InfluxDB pronto per scrittura")
        log_console("=" * 80)
        return True

    except Exception as e:
        log_console(f"\n✗ HEALTH CHECK FAILED: {e}")
        log_console("=" * 80)
        return False


if __name__ == "__main__":
    from tdSynchManager.credentials import get_influx_credentials

    # Get InfluxDB credentials
    influx = get_influx_credentials()
    influx_token = influx['token']
    influx_url = influx.get('url', 'http://127.0.0.1:8181')
    influx_bucket = influx.get('bucket', 'ThetaData')

    healthy = check_influx_health(
        host=influx_url,
        token=influx_token,
        database=influx_bucket
    )

    if healthy:
        log_console("\n[RESULT] InfluxDB è sano e pronto")
    else:
        log_console("\n[RESULT] InfluxDB potrebbe avere problemi - attendere prima di scrivere grosse quantità")
