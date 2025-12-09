# Uso in Jupyter Notebook con Auto-Pulizia Output

Questa guida mostra come prevenire il crash del notebook quando l'output diventa troppo grande durante sincronizzazioni lunghe.

## Quick Start

### 1. Import Base
```python
import sys
sys.path.insert(0, 'path/to/src')  # Adatta al tuo path

from tdSynchManager.output_manager import AutoClearOutputManager
from tdSynchManager.manager import ThetaSyncManager
from tdSynchManager.config import ManagerConfig, Task, DiscoverPolicy
from tdSynchManager.client import ThetaDataV3Client
```

### 2. Uso Più Semplice (Copy-Paste Ready)
```python
# Configurazione normale
influx_token = 'your_token_here'

cfg = ManagerConfig(
    root_dir=r"C:\\path\\to\\data",
    max_concurrency=80,
    influx_url="http://127.0.0.1:8181",
    influx_bucket="ThetaData",
    influx_token=influx_token,
)

tasks = [
    Task(
        asset="option",
        symbols=["SPY", "QQQ", "AAPL"],
        intervals=["1d", "5m"],
        sink="influxdb",
        enrich_bar_greeks=True,
        discover_policy=DiscoverPolicy(mode="mild_skip")
    ),
]

# SOLUZIONE 1: Wrapper con auto-pulizia
async def run_with_autoclear():
    with AutoClearOutputManager(max_lines=50, keep_last_lines=10) as out:
        import builtins
        original_print = builtins.print
        builtins.print = out.print

        try:
            async with ThetaDataV3Client() as client:
                manager = ThetaSyncManager(cfg, client=client)
                await manager.run(tasks)
        finally:
            builtins.print = original_print

# Esegui
await run_with_autoclear()
```

### 3. Controllo Fine-Grained
```python
# Crea manager con limiti personalizzati
output_mgr = AutoClearOutputManager(
    max_lines=100,        # Pulisci dopo 100 righe
    max_chars=50000,      # O dopo 50000 caratteri
    keep_last_lines=20,   # Mantieni ultime 20 righe
    show_stats=True       # Mostra statistiche quando pulisce
)

# Usa .print() invece di print()
for i in range(200):
    output_mgr.print(f"Processing {i}")

# Visualizza statistiche
stats = output_mgr.get_stats()
print(f"Pulizie: {stats['total_clears']}, Righe: {stats['line_count']}")
```

### 4. Context Manager (CONSIGLIATO)
```python
with AutoClearOutputManager(max_lines=50) as out:
    for i in range(200):
        out.print(f"Task {i} completato")
# Auto-cleanup quando esce dal context
```

## Parametri di Configurazione

| Parametro | Default | Descrizione |
|-----------|---------|-------------|
| `max_lines` | 100 | Numero massimo righe prima di pulire |
| `max_chars` | 50000 | Numero massimo caratteri prima di pulire |
| `keep_last_lines` | 20 | Righe da mantenere dopo la pulizia |
| `enabled` | True | Se False, disabilita completamente |
| `show_stats` | True | Mostra stats quando pulisce |

## Esempi Pratici

### Esempio 1: Sincronizzazione Massiva
```python
# Per sincronizzazioni lunghe (100+ simboli)
with AutoClearOutputManager(max_lines=30, keep_last_lines=5) as out:
    import builtins
    original_print = builtins.print
    builtins.print = out.print

    try:
        async with ThetaDataV3Client() as client:
            manager = ThetaSyncManager(cfg, client=client)
            await manager.run(tasks)
    finally:
        builtins.print = original_print
```

### Esempio 2: Debug Mode (Nessuna Pulizia)
```python
# Quando vuoi vedere TUTTO l'output
with AutoClearOutputManager(enabled=False) as out:
    # ... codice ...
    pass
```

### Esempio 3: Solo Summary Finale
```python
from IPython.display import clear_output

async with ThetaDataV3Client() as client:
    manager = ThetaSyncManager(cfg, client=client)
    await manager.run(tasks)

# Pulisci tutto e mostra solo summary
clear_output(wait=True)
print("✓ Sincronizzazione completata!")
```

## Troubleshooting

### Q: "ModuleNotFoundError: No module named 'IPython'"
**A:** L'auto-pulizia funziona solo in Jupyter. In console normale, viene automaticamente disabilitata (nessun errore).

### Q: Output ancora troppo grande
**A:** Riduci `max_lines` o `keep_last_lines`:
```python
AutoClearOutputManager(max_lines=20, keep_last_lines=3)
```

### Q: Voglio vedere tutto senza pulizia
**A:** Usa `enabled=False`:
```python
AutoClearOutputManager(enabled=False)
```

### Q: Come sapere quante volte ha pulito?
**A:** Usa `get_stats()`:
```python
stats = output_mgr.get_stats()
print(f"Pulizie: {stats['total_clears']}")
```

## Best Practices

1. **Per notebook lunghi**: `max_lines=30-50`
2. **Per debug**: `enabled=False`
3. **Per produzione**: `max_lines=100, keep_last_lines=20`
4. **Usa sempre context manager** per cleanup automatico
5. **Monitora stats** se hai crash frequenti

## Integrazione nel Codice Esistente

Se hai già codice tipo:
```python
await manager.run(tasks)
```

Wrappa così:
```python
with AutoClearOutputManager(max_lines=50) as out:
    import builtins
    original_print = builtins.print
    builtins.print = out.print
    try:
        await manager.run(tasks)
    finally:
        builtins.print = original_print
```

## Performance

- **Overhead**: ~0.1ms per print
- **Memoria**: Mantiene buffer di `keep_last_lines` righe
- **Auto-disabilitato**: In console non-Jupyter (zero overhead)
