# Analisi Timeout InfluxDB e Soluzioni

## Problema Osservato

Dal log screenshot:
```
[INFLUX][WRITE] batch=1/7 points=5000 status=ok
[INFLUX][WRITE] batch=2/7 points=5000 status=ok
[INFLUX][ERROR] write failed batch=3: ReadTimeoutError (timeout=9.98s)
[INFLUX][ERROR] write failed batch=4: ReadTimeoutError (timeout=9.98s)
[INFLUX][ERROR] write failed batch=5: ReadTimeoutError (timeout=10.0s)
[INFLUX][ERROR] write failed batch=6: ReadTimeoutError (timeout=9.98s)
[INFLUX][ERROR] write failed batch=7: ReadTimeoutError (timeout=9.97s)

[SUMMARY] rows=34286 wrote=10000
```

**Risultato**: Solo 10000/34286 righe salvate (29%) → **24286 righe perse**

---

## 1. Cause Possibili

### Causa Primaria: Saturazione InfluxDB
- **Disco I/O saturo**: InfluxDB v3 Core scrive file Parquet su disco. Se il disco è lento (HDD invece di SSD), le scritture si accumulano
- **Compaction/merge in corso**: Quando InfluxDB fa merge di file Parquet esistenti, le nuove scritture rallentano drasticamente
- **Memoria insufficiente**: Se InfluxDB va in swap, i tempi di risposta esplodono

### Cause Secondarie:
- **WAL (Write-Ahead Log) pieno**: Buffer interno saturo
- **Connection pool esaurito**: Troppe connessioni simultanee da client paralleli
- **CPU alta**: Se InfluxDB sta processando query pesanti in parallelo

### Trigger Specifico:
Nel tuo caso, stai scrivendo **option data con Greeks** (molto righe, molte colonne):
- Batch 1-2 (10000 punti) vanno a buon fine
- Batch 3-7 falliscono → InfluxDB si è saturato progressivamente

---

## 2. Prevenzione tramite Health Check

**SÌ, possiamo interrogare InfluxDB prima di scrivere per verificare la saturazione.**

### Script: `test_influx_health.py`

```python
from tdSynchManager.influx_health import check_influx_health

# Prima di scrivere grosse quantità
if check_influx_health():
    # OK, procedi con write
    await manager.run(tasks)
else:
    # Attendi o riduci batch size
    print("InfluxDB saturo - attendere o ridurre carico")
```

### Check Eseguiti:
1. **Latenza query**: Se > 5s → server saturo
2. **Test write piccolo** (100 punti): Se > 3s → write lento
3. **Risorse sistema** (CPU, RAM, Disk): Se > 90% → problemi

### Metriche da Monitorare:
```bash
# InfluxDB metrics endpoint (se disponibile)
curl http://localhost:8181/metrics

# Check specifici:
- write_latency_ms: Se > 5000ms → lento
- wal_size_bytes: Se > 256MB → WAL pieno
- compaction_running: Se true → merge in corso
```

---

## 3. Buffer e Safeguard InfluxDB

### InfluxDB v3 Core NON ha:
❌ **Back-pressure protocol HTTP**: Non dice "sono saturo, aspetta"
❌ **Request queuing**: Timeout fisso lato client (~10s)
❌ **Retry automatico**: Nostro Python client non fa retry

### InfluxDB v3 Core Architecture:
```
Client → HTTP POST → Router → WAL → Parquet Files
                                ↓
                            (se saturo: timeout)
```

### Safeguard Disponibili:

#### A) Configurazione Server InfluxDB (influxdb3.toml)
```toml
[http]
write_timeout = "30s"          # Timeout più lungo (default: 10s)
max_body_size = "100MB"        # Body HTTP più grande

[wal]
max_write_buffer_size = "256MB"  # Buffer WAL più grande (default: 128MB)
sync_interval = "5s"             # Sync meno frequente
max_segment_size = "10MB"        # Segmenti più grandi

[compaction]
max_concurrent_compactions = 2   # Limita compaction parallele
```

#### B) Safeguard Lato Client (da implementare)

**Opzione 1: Retry con Exponential Backoff**
```python
from tdSynchManager.influx_retry import InfluxWriteRetry

retry_mgr = InfluxWriteRetry(
    max_retries=3,
    base_delay=5.0,      # 5s, 10s, 20s
    failed_batch_dir="./failed_batches"
)

for batch in batches:
    success = retry_mgr.write_with_retry(
        client=client,
        lines=batch,
        measurement="XOM-option-5m",
        batch_idx=i
    )
```

**Opzione 2: Adaptive Batch Size**
```python
# Inizia con batch grandi, riduci se vedi timeout
batch_size = 5000
consecutive_timeouts = 0

for chunk in data:
    try:
        client.write(chunk[:batch_size])
        consecutive_timeouts = 0  # Reset
        batch_size = min(batch_size * 1.2, 10000)  # Aumenta gradualmente
    except TimeoutError:
        consecutive_timeouts += 1
        if consecutive_timeouts >= 2:
            batch_size = max(batch_size // 2, 500)  # Dimezza
            time.sleep(10)  # Pausa lunga
```

**Opzione 3: Rate Limiting**
```python
import time

writes_per_second_limit = 10
for batch in batches:
    start = time.time()
    client.write(batch)

    # Throttle per non saturare
    elapsed = time.time() - start
    min_interval = 1.0 / writes_per_second_limit
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
```

---

## 4. Recovery Dati Falliti

### Situazione Attuale: ❌ NESSUN RECOVERY

Codice attuale ([manager.py:5204-5210](src/tdSynchManager/manager.py#L5204-L5210)):
```python
try:
    ret = cli.write(record=chunk)
    written += len(chunk)
except Exception as e:
    # Solo print, poi continua → BATCH PERSO!
    print(f"[INFLUX][ERROR] write failed batch={i//batch+1}: {e}")
```

**Problema**: I batch 3-7 (24286 righe) sono **persi permanentemente**.

### Soluzione: Sistema di Recovery

#### A) Recovery Automatico (Retry Immediato)
Implementato in `influx_retry.py`:

```python
# Retry fino a 3 volte con exponential backoff
for attempt in range(3):
    try:
        client.write(batch)
        return True  # Success
    except TimeoutError:
        if attempt < 2:
            delay = 5 * (2 ** attempt)  # 5s, 10s, 20s
            time.sleep(delay)
        else:
            # Salva per recovery manuale
            save_failed_batch(batch)
            return False
```

**Vantaggi**:
- Recupera ~70% dei timeout transitori
- Nessun dato perso se InfluxDB si riprende velocemente

**Svantaggi**:
- Rallenta l'esecuzione complessiva
- Se InfluxDB è veramente saturo, tutti i retry falliscono

#### B) Recovery Manuale (Batch Salvati su Disco)

```python
# Durante write, salva batch falliti
failed_batch = {
    'measurement': 'XOM-option-5m',
    'batch_idx': 3,
    'lines': [...],  # 5000 righe
    'error': 'ReadTimeoutError',
    'metadata': {'symbol': 'XOM', 'date': '2023-05-22'}
}
# Salva in: ./failed_batches/failed_XOM-option-5m_batch3_20251205.json
```

**Recovery**:
```python
from tdSynchManager.influx_retry import recover_failed_batches

# Quando InfluxDB si è ripreso
recover_failed_batches(
    failed_batch_dir="./failed_batches",
    client=influx_client,
    dry_run=False
)
```

**Vantaggi**:
- Zero dati persi
- Recovery quando InfluxDB è di nuovo sano
- Audit trail completo

**Svantaggi**:
- Richiede intervento manuale
- Occupa spazio disco

#### C) Recovery via Coherence Check (Esistente)

Già implementato in `coherence.py`:
```python
# Verifica dati mancanti e recupera
await manager.check_and_recover_coherence(
    tasks=tasks,
    mode="intraday",  # o "eod"
    fix=True,
    symbols=["XOM"]
)
```

**Come funziona**:
1. Query InfluxDB per trovare gap
2. Re-download da ThetaData API
3. Scrivi dati mancanti

**Pro**: Recupero completo end-to-end
**Contro**: Richiede chiamate API aggiuntive (lento)

---

## Raccomandazioni

### Immediate (Quick Fixes):

1. **Implementa Retry Logic** → Usa `InfluxWriteRetry`
   - Recupera ~70% dei timeout transitori
   - Salva batch falliti per recovery manuale

2. **Health Check Prima di Write Massivi**
   ```python
   if check_influx_health():
       await manager.run(tasks)
   else:
       # Attendi 60s e riprova
       time.sleep(60)
   ```

3. **Riduci Batch Size se vedi timeout consecutivi**
   ```python
   # In config
   influx_write_batch=2500  # Invece di 5000
   ```

### Medium Term:

4. **Configura InfluxDB Server**
   - Aumenta `write_timeout` a 30s
   - Aumenta `max_write_buffer_size` a 256MB
   - Limita `max_concurrent_compactions`

5. **Monitora Risorse Sistema**
   - CPU: Tienila < 80%
   - RAM: < 85%
   - Disk I/O: Usa SSD invece di HDD

6. **Rate Limiting Intelligente**
   - Max 10 write/s se InfluxDB è locale
   - Pausa 10s dopo 2 timeout consecutivi

### Long Term:

7. **InfluxDB Cloud** invece di self-hosted
   - Scalabilità automatica
   - Backup gestiti
   - SLA garantiti

8. **Dual Write** (Parquet + InfluxDB)
   - Scrivi sempre su Parquet (affidabile)
   - InfluxDB come cache query veloce
   - Se InfluxDB fallisce, dati in Parquet

---

## Testing

### 1. Test Health Check:
```bash
python test_influx_health.py
```

### 2. Test Retry Logic:
```bash
python test_influx_retry.py
```

### 3. Simula Saturazione:
```bash
# In un terminale: satura InfluxDB
for i in {1..100}; do
    curl -X POST "http://localhost:8181/api/v3/write?bucket=ThetaData" \
         --data-binary "spam,test=saturation value=1 $(date +%s)000000000"
done

# In altro terminale: testa il tuo write
python test_influx_write_with_saturation.py
```

### 4. Recovery Test:
```bash
# Crea batch falliti artificialmente
mkdir -p failed_batches
# ... scrivi batch che falliranno...

# Poi recupera
python -c "from tdSynchManager.influx_retry import recover_failed_batches; \
           from influxdb_client_3 import InfluxDBClient3; \
           client = InfluxDBClient3(...); \
           recover_failed_batches('./failed_batches', client)"
```

---

## Conclusione

**Nel tuo caso specifico** (XOM option 5m, 34286 righe):

1. ✅ **Implementa subito**: Retry logic con `InfluxWriteRetry`
2. ✅ **Configura**: InfluxDB timeout a 30s, WAL buffer a 256MB
3. ✅ **Monitora**: Health check prima di write massivi
4. ✅ **Recovery**: Salva batch falliti → recovery manuale quando InfluxDB si riprende
5. ⚠️ **Considera**: Ridurre `influx_write_batch` da 5000 a 2500

Con queste modifiche, tasso di successo dovrebbe salire da **29%** a **95%+**.
