#!/bin/bash

echo "=========================================="
echo "PULIZIA COMPLETA CACHE PYTHON"
echo "=========================================="

cd "$(dirname "$0")"

echo ""
echo "1. Cancellazione cache bytecode __pycache__..."
find . -type d -name "__pycache__" | while read dir; do
    echo "  Removing: $dir"
    rm -rf "$dir"
done

echo ""
echo "2. Cancellazione file .pyc..."
find . -name "*.pyc" | while read file; do
    echo "  Removing: $file"
    rm -f "$file"
done

echo ""
echo "3. Disinstallazione pacchetto installato (se presente)..."
pip uninstall -y tdsynchmanager 2>/dev/null || echo "  (non installato)"
pip uninstall -y tdSynchManager 2>/dev/null || echo "  (non installato)"

echo ""
echo "4. Verifica modifiche timezone nel codice..."
count=$(grep -c 'tz_localize("America/New_York")' src/tdSynchManager/manager.py 2>/dev/null || echo "0")
if [ "$count" -ge "2" ]; then
    echo "  ✓ Timezone fix presente ($count occorrenze)"
else
    echo "  ✗ PROBLEMA: Timezone fix NON trovato!"
fi

echo ""
echo "5. Trova tutte le copie di manager.py..."
find . -name "manager.py" -type f | grep -v __pycache__

echo ""
echo "=========================================="
echo "PULIZIA COMPLETATA!"
echo "=========================================="
echo ""
echo "PROSSIMI PASSI:"
echo "1. In Jupyter: Kernel > Restart & Clear Output"
echo "2. Esegui il codice di reload in FORCE_RELOAD_INSTRUCTIONS.md"
echo "3. Cancella le tabelle InfluxDB"
echo "4. Ri-esegui il download"
echo ""
