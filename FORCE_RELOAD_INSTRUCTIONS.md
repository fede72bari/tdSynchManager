# ISTRUZIONI PER FORZARE IL RELOAD DEL CODICE

## Problema
Il codice è stato corretto nel file sorgente, ma Jupyter continua a usare una versione vecchia cached.

## Soluzione Completa

### 1. Cancella TUTTE le cache bytecode Python
```bash
# Trova e cancella TUTTE le cache .pyc
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -name "*.pyc" -delete 2>/dev/null || true
```

### 2. Disinstalla il pacchetto se installato
```bash
pip uninstall -y tdsynchmanager tdSynchManager
```

### 3. In Jupyter: Kernel > Restart & Clear Output

### 4. In Jupyter: Esegui questo codice PER PRIMO
```python
import sys
from pathlib import Path

# Rimuovi eventuali riferimenti a tdSynchManager già caricati
modules_to_remove = [key for key in sys.modules if 'tdSynchManager' in key or 'tdsynchmanager' in key]
for mod in modules_to_remove:
    del sys.modules[mod]
    print(f"Removed module: {mod}")

# Assicurati che il path sia pulito
PKG_SRC = r"D:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager\src"

# Rimuovi eventuali vecchi path
sys.path = [p for p in sys.path if 'tdSynchManager' not in p and 'tdsynchmanager' not in p]

# Aggiungi il path corretto PRIMA di tutto
sys.path.insert(0, PKG_SRC)

print(f"Python path cleaned and PKG_SRC added: {PKG_SRC}")
print(f"sys.path[0] = {sys.path[0]}")

# ORA importa
from tdSynchManager import ThetaSyncManager, ThetaDataV3Client, ManagerConfig, Task

# Verifica quale file è stato caricato
import tdSynchManager.manager as mgr_module
import inspect
source_file = inspect.getsourcefile(mgr_module.ThetaSyncManager)
print(f"\n✓ Loaded ThetaSyncManager from: {source_file}")

# Verifica che la modifica timezone sia presente
source_code = inspect.getsource(mgr_module.ThetaSyncManager._download_and_store_options)
if 'tz_localize("America/New_York")' in source_code:
    count = source_code.count('tz_localize("America/New_York")')
    print(f"✓ Timezone fix IS present ({count} occurrences)")
else:
    print("✗ Timezone fix NOT present - PROBLEMA!")
```

### 5. Se ancora non funziona
Verifica che non ci siano altre copie del file:
```bash
find . -name "manager.py" -type f | grep -v __pycache__
```

### 6. Verifica che la modifica sia presente nel file
```bash
grep -n 'tz_localize("America/New_York")' src/tdSynchManager/manager.py
```
Dovrebbe mostrare 3 occorrenze (riga ~2065, ~3297, e nella funzione di conversione InfluxDB)

## Verifica Finale
Dopo aver fatto tutto questo:
1. Cancella le tabelle InfluxDB
2. Ri-esegui il download
3. Controlla i log - dovresti vedere i messaggi `[INFLUX-TS-CONV]` con le informazioni di debug
4. Verifica che i log mostrino "timezone-aware (America/New_York)"
