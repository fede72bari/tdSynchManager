"""
Test per AutoClearOutputManager
Dimostra la pulizia automatica dell'output in Jupyter
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import time
from tdSynchManager.output_manager import AutoClearOutputManager

print("=" * 80)
print("TEST: AutoClearOutputManager")
print("=" * 80)

# Test 1: Uso base con context manager
print("\n[TEST 1] Uso base con max_lines=20")
print("-" * 80)

with AutoClearOutputManager(max_lines=20, keep_last_lines=5, show_stats=True) as out:
    for i in range(50):
        out.print(f"[{i:03d}] Processing task {i}")
        time.sleep(0.05)  # Simula lavoro

print("\n" + "=" * 80)

# Test 2: Con limite caratteri
print("\n[TEST 2] Limite su caratteri (max_chars=500)")
print("-" * 80)

with AutoClearOutputManager(max_chars=500, keep_last_lines=3) as out:
    for i in range(30):
        # Stampa righe lunghe
        out.print(f"[{i:03d}] " + "X" * 50)
        time.sleep(0.03)

print("\n" + "=" * 80)

# Test 3: Uso senza context manager
print("\n[TEST 3] Uso diretto senza context manager")
print("-" * 80)

mgr = AutoClearOutputManager(max_lines=15, keep_last_lines=5, show_stats=True)

for i in range(40):
    mgr.print(f"Item {i:03d} - Status: OK")
    time.sleep(0.02)

stats = mgr.get_stats()
print(f"\n[STATS] Pulizie totali: {stats['total_clears']}")
print(f"[STATS] Tempo totale: {stats['elapsed_time']:.2f}s")
print(f"[STATS] Righe correnti: {stats['line_count']}")
print(f"[STATS] Caratteri correnti: {stats['char_count']}")

print("\n" + "=" * 80)

# Test 4: Disabilitato (nessuna pulizia)
print("\n[TEST 4] Manager disabilitato (enabled=False)")
print("-" * 80)

with AutoClearOutputManager(max_lines=10, enabled=False) as out:
    for i in range(25):
        out.print(f"Line {i} - NEVER CLEARS")
        time.sleep(0.02)

print("\n[TEST 4] Tutte le 25 righe dovrebbero essere visibili sopra")

print("\n" + "=" * 80)
print("TEST COMPLETATO")
print("=" * 80)
