"""
Test per AutoClearOutputManager
Dimostra la pulizia automatica dell'output in Jupyter
"""
from console_log import log_console

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import time
from tdSynchManager.output_manager import AutoClearOutputManager

log_console("=" * 80)
log_console("TEST: AutoClearOutputManager")
log_console("=" * 80)

# Test 1: Uso base con context manager
log_console("\n[TEST 1] Uso base con max_lines=20")
log_console("-" * 80)

with AutoClearOutputManager(max_lines=20, keep_last_lines=5, show_stats=True) as out:
    for i in range(50):
        out.print(f"[{i:03d}] Processing task {i}")
        time.sleep(0.05)  # Simula lavoro

log_console("\n" + "=" * 80)

# Test 2: Con limite caratteri
log_console("\n[TEST 2] Limite su caratteri (max_chars=500)")
log_console("-" * 80)

with AutoClearOutputManager(max_chars=500, keep_last_lines=3) as out:
    for i in range(30):
        # Stampa righe lunghe
        out.print(f"[{i:03d}] " + "X" * 50)
        time.sleep(0.03)

log_console("\n" + "=" * 80)

# Test 3: Uso senza context manager
log_console("\n[TEST 3] Uso diretto senza context manager")
log_console("-" * 80)

mgr = AutoClearOutputManager(max_lines=15, keep_last_lines=5, show_stats=True)

for i in range(40):
    mgr.print(f"Item {i:03d} - Status: OK")
    time.sleep(0.02)

stats = mgr.get_stats()
log_console(f"\n[STATS] Pulizie totali: {stats['total_clears']}")
log_console(f"[STATS] Tempo totale: {stats['elapsed_time']:.2f}s")
log_console(f"[STATS] Righe correnti: {stats['line_count']}")
log_console(f"[STATS] Caratteri correnti: {stats['char_count']}")

log_console("\n" + "=" * 80)

# Test 4: Disabilitato (nessuna pulizia)
log_console("\n[TEST 4] Manager disabilitato (enabled=False)")
log_console("-" * 80)

with AutoClearOutputManager(max_lines=10, enabled=False) as out:
    for i in range(25):
        out.print(f"Line {i} - NEVER CLEARS")
        time.sleep(0.02)

log_console("\n[TEST 4] Tutte le 25 righe dovrebbero essere visibili sopra")

log_console("\n" + "=" * 80)
log_console("TEST COMPLETATO")
log_console("=" * 80)
