"""
Test minimale: scarica UN SOLO giorno per vedere i debug log.
"""
from console_log import log_console

import asyncio
import sys
from pathlib import Path

# Pulisci imports
keys_to_remove = [k for k in list(sys.modules.keys()) if 'tdsyn' in k.lower()]
for k in keys_to_remove:
    del sys.modules[k]

# Setup path
PKG_SRC = Path(__file__).parent / "src"
sys.path.insert(0, str(PKG_SRC))

from tdSynchManager import ThetaSyncManager, ManagerConfig, Task, ThetaDataV3Client

async def test_single_day():
    log_console("=" * 80)
    log_console("TEST: Single day download con debug logging")
    log_console("=" * 80)

    config = ManagerConfig(
        root_dir="./test_output",
        max_file_mb=50,
        overlap_seconds=0,
    )

    client = ThetaDataV3Client()
    manager = ThetaSyncManager(cfg=config, client=client)

    # Scarica SOLO 2026-01-05
    task = Task(
        asset="option",
        symbols=["AAL"],
        intervals=["1d"],
        first_date_override="2026-01-05",
        end_date_override="2026-01-05",  # SOLO UN GIORNO
        sink="influxdb",
        enrich_bar_greeks=True,
        use_api_date_discovery=True,
    )

    log_console(f"\nDownload: {task.symbols[0]} {task.intervals[0]} date={task.first_date_override}")
    log_console("\nCerca questi log:")
    log_console("  [DEBUG-EDOI-1] Created effective_date_oi")
    log_console("  [DEBUG-EDOI-2] After normalize")
    log_console("  [DEBUG-EDOI-3-4-5] Merge process")
    log_console("  [DEBUG-EDOI-6-7] Before/After _ensure_ts_utc_column")
    log_console("  [INFLUX-TS-CONV] Processing column 'effective_date_oi'")
    log_console("")

    try:
        await manager.run(tasks=[task])
        log_console("\n" + "=" * 80)
        log_console("TEST COMPLETATO - Controlla i log sopra")
        log_console("=" * 80)
    except Exception as e:
        log_console(f"\nERRORE: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await manager.close()

if __name__ == "__main__":
    asyncio.run(test_single_day())
