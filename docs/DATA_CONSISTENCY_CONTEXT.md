# Data Consistency System - Contesto Implementazione

**Branch**: `claude/data-consistency-fallbacks-011BHdqNzv7C8nJX5qPKKTJU`
**Data Implementazione**: 20-21 Novembre 2025
**Stato**: ✅ Completato e mergiato in main (22 Nov 2025)

---

## Panoramica Sistema

Sistema completo di validazione e recovery per garantire coerenza dati tra storage locale e ThetaData API.

**Due modalità operative:**
1. **Real-Time Validation**: Durante download storico (in-RAM, prima del salvataggio)
2. **Post-Hoc Coherence Checking**: Verifica DB esistenti e recovery automatico

---

## Componenti Implementati

### Nuovi Moduli

| File | Righe | Descrizione |
|------|-------|-------------|
| `exceptions.py` | 34 | Eccezioni custom (SessionClosedError, TruncatedResponseError, ValidationError, InfluxDBAuthError) |
| `logger.py` | 481 | DataConsistencyLogger - logging strutturato su Parquet |
| `retry.py` | 146 | retry_with_policy() - retry con backoff configurabile |
| `validator.py` | 482 | DataValidator - validazione completezza dati (stateless) |
| `coherence.py` | 1251 | CoherenceChecker + IncoherenceRecovery - verifica post-hoc |

### Moduli Modificati

| File | Modifiche | Descrizione |
|------|-----------|-------------|
| `config.py` | +34 righe | RetryPolicy dataclass, parametri validazione in ManagerConfig |
| `client.py` | +275 righe | ResilientThetaClient wrapper con auto-reconnect |
| `manager.py` | +566 righe | Integrazione validazione, metodi helper, check_and_recover_coherence() |

---

## Funzionalità Principali

### 1. Real-Time Validation (Durante Download)

**Location**: `manager.py` - `_validate_downloaded_data()`

**Trigger**: Automatico prima di ogni salvataggio (CSV/Parquet/InfluxDB)

**Validazioni**:
- ✅ Required columns (timestamp, OHLC, volume, contract keys)
- ✅ Enrichment columns (delta, gamma, theta, vega, rho, IV)
- ✅ EOD completeness (verifica date mancanti)
- ✅ Intraday completeness (conta candele vs attese, tolleranza 5%)
- ✅ Tick vs EOD volume (con separazione call/put per options, tolleranza 1%)

**Strict Mode**:
- `validation_strict_mode=True`: Blocca salvataggio se validation fallisce ("all-or-nothing")
- `validation_strict_mode=False`: Salva comunque ma logga warning

**Integrazione**:
- `_download_and_store_options()` - EOD (linea ~1351), Intraday (linea ~2114)
- `_download_and_store_equity_or_index()` - Stock/Index (linea ~2486)

### 2. Post-Hoc Coherence Checking

**Location**: `coherence.py` - `CoherenceChecker`, `IncoherenceRecovery`

**Uso**:
```python
report = await manager.check_and_recover_coherence(
    symbol="AAPL",
    asset="stock",
    interval="5m",
    sink="parquet",
    start_date="2024-01-01",
    end_date="2024-01-31",
    auto_recover=True  # Recupera automaticamente
)
```

**Funzionalità**:
- Legge dati da DB locale (CSV/Parquet/InfluxDB)
- Identifica missing days (EOD)
- Identifica missing candles (intraday) con **segmentazione 30 minuti**
- Valida tick vs EOD volumes con **segmentazione 1 ora**
- Recovery con **range abbondanti** (±1 giorno di margine)
- Verifica locale prima di re-download (evita API calls inutili)

**Output**: `CoherenceReport` con:
- `is_coherent: bool`
- `issues: List[CoherenceIssue]` (tipo, severity, date_range, descrizione, dettagli)
- `missing_days`, `intraday_gaps`, `tick_volume_mismatches`

### 3. Logging Strutturato

**Location**: `{root_dir}/logs/data_consistency/`

**Naming**: `log_{table_name}.parquet` (es. `log_stock_AAPL_5m.parquet`)

**Schema**:
```python
{
    'timestamp': datetime,           # UTC quando evento occorso
    'event_type': str,               # VALIDATION_FAILURE, RETRY_ATTEMPT, RESOLUTION
    'severity': str,                 # INFO, WARNING, ERROR, CRITICAL
    'symbol': str,
    'asset': str,                    # stock, option, index
    'interval': str,                 # 5m, 1d, tick
    'date_range_start': str,
    'date_range_end': str,
    'error_message': str,
    'retry_attempt': int,
    'resolution_status': str,        # PENDING, RESOLVED, FAILED
    'details_json': str              # JSON con dettagli aggiuntivi
}
```

### 4. Retry Policy Configurabile

**Classe**: `RetryPolicy` (in `config.py`)

```python
@dataclass
class RetryPolicy:
    max_attempts: int = 3
    delay_seconds: float = 15.0
    truncated_response_delay: float = 60.0
    truncated_max_attempts: int = 2
    session_closed_max_attempts: int = 1
```

**Wrapper**: `retry_with_policy()` in `retry.py`
- Retry generico su qualsiasi coroutine
- Logging automatico tentativi/risoluzioni
- Backoff configurabile

### 5. Session Recovery

**Wrapper**: `ResilientThetaClient` (in `client.py`)

**Funzionalità**:
- Detect "session closed" errors
- Auto-reconnect (max 1 tentativo di default)
- Logging sessioni chiuse
- Termina processo se reconnect fallisce

### 6. Gestione Errori Specifici

**Risposte Troncate/Incomplete**:
- Rilevamento automatico (dimensione sospetta, parser errors)
- Pausa globale 60s
- Retry max 2 volte
- Terminazione se fallisce

**InfluxDB Unauthorized**:
- Logging + terminazione immediata processo

---

## Architettura

### Grafo Dipendenze

```
config.py (no dependencies)
    │
    ├─► exceptions.py ──┐
    │                   │
    ├─► logger.py ──────┼─► validator.py
    │                   │
    │                   ├─► retry.py
    │                   │
    │                   └─► coherence.py
    │
    └─► client.py (ResilientThetaClient)
         │
         └─► manager.py (orchestrator)
```

### Design Principles

- **Separazione responsabilità**: Ogni modulo ha scopo chiaro
- **Testabilità**: Componenti testabili isolatamente
- **Stateless validators**: Funzioni pure (DataFrame → ValidationResult)
- **Dependency Injection**: Logger/Client iniettati nel Manager
- **Composabilità**: retry_with_policy() riusabile ovunque

---

## Configurazione

```python
from tdSynchManager import ManagerConfig, RetryPolicy

cfg = ManagerConfig(
    root_dir="./data",

    # Validation settings
    enable_data_validation=True,
    validation_strict_mode=True,           # All-or-nothing
    tick_eod_volume_tolerance=0.01,        # 1% tolerance
    log_verbose_console=True,

    # Retry policy
    retry_policy=RetryPolicy(
        max_attempts=3,
        delay_seconds=15.0,
        truncated_response_delay=60.0,
        truncated_max_attempts=2,
        session_closed_max_attempts=1
    )
)
```

---

## Test Effettuati

### Durante Implementazione

✅ **Syntax check**: `python3 -m py_compile manager.py` (tutti i moduli)
✅ **Integration test**: Validazione funziona su download reale
✅ **Coherence check**: Verifica DB esistenti identifica problemi correttamente
✅ **Recovery test**: Auto-recovery scarica dati mancanti con range abbondanti
✅ **Logging test**: Log strutturati scritti correttamente su Parquet

### Test NON Completati

⚠️ **Unit tests formali**: Nessun test pytest/unittest creato
⚠️ **Stress test**: Non testato su scenari di failure massivo
⚠️ **Performance test**: Non misurato impatto su performance download
⚠️ **Edge cases**:
- Market holidays (early close)
- Options expiration days (different hours)
- Premarket/afterhours data
- Multiple concurrent failures

---

## Limitazioni Conosciute

1. **Segmentazione Intraday**: Fixed 30min blocks - potrebbe non allinearsi con problemi reali
2. **EOD Volume per Tick**: Se EOD non esiste né in DB né su API, tick validation skip
3. **Options Volume Separation**: Assume colonna 'right' con valori 'C'/'P' - non robusto per formati diversi
4. **InfluxDB Verification**: Query post-insert può essere lenta su DB grandi
5. **Recovery Range**: ±1 giorno fisso - potrebbe scaricare troppo/poco

---

## Modifiche Future Consigliate

### Priorità Alta

1. **Unit Tests**: Creare test suite completa per validator, logger, retry
2. **Performance Optimization**: Caching EOD volumes, batch validation
3. **Configurability**: Rendere configurabili:
   - Segmentation intervals (30min, 1h)
   - Recovery margins (±1 day)
   - Tolerance thresholds per asset/interval

### Priorità Media

4. **Enhanced Segmentation**: Adaptive segmentation basata su gap size
5. **InfluxDB Batch Verification**: Verifica batch invece di query per ogni inserimento
6. **Retry Backoff**: Exponential backoff invece di fixed delay
7. **Market Calendar**: Integrazione con market calendar per holiday handling

### Priorità Bassa

8. **Dashboard**: Web UI per visualizzare log e coherence reports
9. **Alerts**: Notifiche (email/Slack) per errori critici
10. **Metrics**: Prometheus metrics per monitoring

---

## Commit History

```
fe35869 - Add data consistency and fallback system
fb6f41a - Add .gitignore to exclude Python cache files
0faf7f5 - Enhance docstrings to match project style
56fc5c6 - Reorganize project structure for better maintainability
2dbe802 - Remove debug log files (moved to .archive/)
b45755e - Enhance options volume validation and optimize recovery
2fd6911 - Complete coherence checking and recovery implementation
96674df - Integrate real-time data validation in manager download methods
c79fe98 - changes (final merge commit)
```

**Merged to main**: 22 Nov 2025 (9 commits fast-forward)

---

## Uso Operativo

### Download Storico con Validation

```python
# Come prima - validation automatica!
await manager.run(tasks)
```

Log automatici in `{root_dir}/logs/data_consistency/`

### Check DB Esistenti

```python
# Verifica e recupera
report = await manager.check_and_recover_coherence(
    symbol="AAPL",
    asset="stock",
    interval="5m",
    sink="parquet",
    start_date="2024-01-01",
    end_date="2024-01-31",
    auto_recover=True  # False = solo verifica
)

# Analizza risultati
if not report.is_coherent:
    for issue in report.issues:
        print(f"{issue.issue_type}: {issue.description}")
        if 'problem_segments' in issue.details:
            for seg in issue.details['problem_segments']:
                print(f"  {seg['segment_start']}-{seg['segment_end']}: {seg['issue']}")
```

---

## AGGIORNAMENTO 22 Nov 2025 - Problemi Identificati e Fix

### Problemi Critici Post-Merge

Dopo il merge in main, sono stati identificati 3 problemi critici nell'implementazione originale:

#### **PROBLEMA 1: Retry NON Implementato in Real-Time Validation** ❌

**Stato Originale**:
- `_validate_downloaded_data()` valida ma NON ritenta download se fallisce
- Se validation fallisce → skip giorno e va avanti
- **Requisito mancante**: N tentativi (default 3) con delay (default 15s)

**Fix Implementata** ✅:
- Nuovo modulo: `download_retry.py`
- Funzione: `download_with_retry_and_validation()`
- Centralizza retry logic con validation integrata
- Retry automatico download + re-validation fino a N tentativi

#### **PROBLEMA 2: InfluxDB Auth Gestito da Client invece che Manager** ❌

**Stato Originale**:
- `InfluxDBAuthError` definita in `exceptions.py` ma NON catturata
- Manager scrive su InfluxDB ma non gestisce errori auth
- **Problema architetturale**: Client fornisce dati, Manager gestisce storage

**Fix Richiesta** ⚠️:
- Aggiungere try/catch in `_append_influx_df()` per `InfluxDBAuthError`
- Loggare errore + terminare TUTTI i task in corso
- Implementare in `manager.py` (TODO)

#### **PROBLEMA 3: Nessuna Verifica Scrittura Parziale InfluxDB** ❌

**Stato Originale**:
- `_append_influx_df()` scrive ma NON verifica cosa è stato effettivamente salvato
- Nessuna query post-write per verificare righe scritte
- **Requisito mancante**: Identificare dati mancanti, retry K volte solo su mancanti

**Fix Implementata** ✅:
- Nuovo modulo: `influx_verification.py`
- Funzione: `verify_influx_write()` - query post-write per verificare righe
- Funzione: `write_influx_with_verification()` - write + verify + retry granulare
- Retry solo su righe mancanti (non tutto il batch)

### Nuovi Moduli Aggiunti (22 Nov 2025)

| File | Righe | Descrizione |
|------|-------|-------------|
| `download_retry.py` | ~150 | Retry centralizzato download + validation |
| `influx_verification.py` | ~320 | Verifica post-write InfluxDB + retry granulare |

### Stato Implementazione Fix

| Fix | Modulo Creato | Integrato in Manager | Testato |
|-----|---------------|----------------------|---------|
| Retry real-time validation | ✅ | ⚠️ Pending | ❌ |
| InfluxDB auth handling | ❌ N/A | ❌ | ❌ |
| InfluxDB write verification | ✅ | ⚠️ Pending | ❌ |

### Integrazione Richiesta

Per completare le fix, serve integrare i nuovi moduli nei punti di download esistenti:

**1. Download con Retry + Validation** (da fare in `manager.py`):

```python
from .download_retry import download_with_retry_and_validation

# Sostituire nei punti di download EOD/intraday/tick:
# PRIMA (senza retry):
result, url = await self.client.option_history_eod(...)
df = pd.read_csv(StringIO(result[0]))
validation_ok = await self._validate_downloaded_data(df, ...)
if not validation_ok:
    return  # Skip

# DOPO (con retry):
df, success = await download_with_retry_and_validation(
    download_func=lambda: self.client.option_history_eod(...),
    parse_func=lambda result: pd.read_csv(StringIO(result[0])),
    validate_func=lambda df: self._validate_downloaded_data(df, ...),
    retry_policy=self.cfg.retry_policy,
    logger=self.logger,
    context={'symbol': symbol, 'asset': asset, ...}
)
if not success:
    return  # Skip after N retries
```

**2. InfluxDB con Verifica** (da fare in `manager.py`):

```python
from .influx_verification import write_influx_with_verification, verify_influx_write

# Sostituire:
# PRIMA (senza verifica):
wrote = await self._append_influx_df(base_path, df)

# DOPO (con verifica + retry):
wrote, success = await write_influx_with_verification(
    write_func=lambda df: self._append_influx_df(base_path, df),
    verify_func=lambda df: verify_influx_write(
        self._ensure_influx_client(),
        measurement,
        df,
        key_cols=['__ts_utc', 'symbol', 'strike', 'expiration', 'right']
    ),
    df=df,
    retry_policy=self.cfg.retry_policy,
    logger=self.logger,
    context={'symbol': symbol, 'measurement': measurement, ...}
)

if not success:
    # Log permanent failure + continue
    pass
```

**3. InfluxDB Auth Error Handling** (da fare in `_append_influx_df`):

```python
from .exceptions import InfluxDBAuthError

async def _append_influx_df(self, base_path: str, df_new) -> int:
    try:
        # ... existing write logic ...
    except Exception as e:
        # Check for auth errors
        if 'unauthorized' in str(e).lower() or '401' in str(e) or '403' in str(e):
            self.logger.log_influx_failure(
                "AUTH_FAILURE",
                fatal=True,
                details={'error': str(e)}
            )
            raise InfluxDBAuthError(f"InfluxDB authentication failed: {e}")
        raise
```

### TODO - Integrazione Completa

- [ ] Integrare `download_with_retry_and_validation()` in tutti i punti download
- [ ] Integrare `write_influx_with_verification()` in `_append_influx_df()`
- [ ] Aggiungere catch `InfluxDBAuthError` in manager run loop
- [ ] Test end-to-end con retry failures
- [ ] Test InfluxDB write verification
- [ ] Update CHANGELOG con fix

---

## Riferimenti

- **Discovery Behavior**: Vedi `DISCOVERY_BEHAVIOR_ANALYSIS.md`
- **Docstrings Status**: Vedi `DOCSTRING_UPDATE_STATUS.md`
- **Conversazione Completa**: `conversazione contesto claude.txt` (5920 righe)

---

**Nota**: Questo documento è stato generato automaticamente analizzando la conversazione completa con Claude web (20-21 Nov 2025).
