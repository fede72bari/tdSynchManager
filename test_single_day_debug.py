"""
Test minimale: scarica UN SOLO giorno per vedere i debug log.
"""

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
    print("=" * 80)
    print("TEST: Single day download con debug logging")
    print("=" * 80)

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

    print(f"\nDownload: {task.symbols[0]} {task.intervals[0]} date={task.first_date_override}")
    print("\nCerca questi log:")
    print("  [DEBUG-EDOI-1] Created effective_date_oi")
    print("  [DEBUG-EDOI-2] After normalize")
    print("  [DEBUG-EDOI-3-4-5] Merge process")
    print("  [DEBUG-EDOI-6-7] Before/After _ensure_ts_utc_column")
    print("  [INFLUX-TS-CONV] Processing column 'effective_date_oi'")
    print("")

    try:
        await manager.run(tasks=[task])
        print("\n" + "=" * 80)
        print("TEST COMPLETATO - Controlla i log sopra")
        print("=" * 80)
    except Exception as e:
        print(f"\nERRORE: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await manager.close()

if __name__ == "__main__":
    asyncio.run(test_single_day())
