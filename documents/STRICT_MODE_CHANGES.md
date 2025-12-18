# Modifiche Strict Mode per Option Greeks/IV/OI

## Sommario

Implementato il **strict mode** per le opzioni quando `enrich_greeks=True`: se Greeks/IV/OI falliscono, il sistema **skippa il salvataggio** invece di salvare dati parziali. Questo permette al coherence check di identificare correttamente i dati mancanti e tentare il recovery.

## Modifiche Implementate

### 1. EOD Flow (manager.py, linee ~1608-1810)

**Comportamento precedente:**
- Se greeks/OI download fallivano → salvava comunque OHLC (dati parziali)
- Colonne greeks/OI rimanevano NaN
- Coherence check NON rilevava il problema

**Nuovo comportamento (strict mode):**
- Se `enrich_greeks=True` e greeks download fallisce → **raise ValueError** (skip save)
- Se greeks CSV è vuoto → **raise ValueError** (skip save)
- Se greeks merge fallisce (no join keys) → **raise ValueError** (skip save)
- Se OI download fallisce → **raise ValueError** (skip save)
- Se OI CSV è vuoto → **raise ValueError** (skip save)
- Se OI merge genera exception → **raise** (skip save)

**Log output:**
```
[SKIP-SAVE] option EOD AAL 2022-01-03: greeks download failed, skipping to allow coherence check detection
[VALIDATION] STRICT MODE: Skipping save for option AAL EOD 2022-01-03 - all retry attempts failed
```

**Log parquet:**
- `event_type`: `GREEKS_DOWNLOAD_FAILED`, `GREEKS_CSV_EMPTY`, `GREEKS_MERGE_FAILED`, `OI_DOWNLOAD_FAILED`, `OI_CSV_EMPTY`, `OI_MERGE_ERROR`
- `severity`: `ERROR`

---

### 2. Intraday Flow (manager.py, linee ~2205-2354)

**Comportamento precedente:**
- Se greeks/IV download fallivano per expiration → salvava comunque OHLC (dati parziali)
- Colonne greeks/IV rimanevano NaN
- Coherence check NON rilevava il problema

**Nuovo comportamento (strict mode):**
- **Per ogni expiration:**
  - Traccia se greeks/IV hanno successo
  - Se greeks download fallisce → **skip expiration** (NON appende OHLC a lista)
  - Se greeks CSV è vuoto → **skip expiration**
  - Se IV download fallisce → **skip expiration**
  - Se IV CSV è vuoto → **skip expiration**
  - Se IV exception → **skip expiration**
- **Se TUTTE le expirations sono skippate:**
  - Log `[SKIP-DAY]` + log parquet con `ALL_EXPIRATIONS_FAILED`
  - Return senza salvare nulla

**Log output (per expiration):**
```
[SKIP-EXPIRATION] option intraday AAL 5m 2022-01-03 exp=20220916: greeks download failed
[SKIP-EXPIRATION] option intraday AAL 5m 2022-01-03 exp=20220916: IV download failed
[SKIP-EXPIRATION] option intraday AAL 5m 2022-01-03 exp=20220916: OHLC not saved due to incomplete greeks/IV
```

**Log output (giorno intero):**
```
[SKIP-DAY] option intraday AAL 5m 2022-01-03: no data saved (all expirations failed/skipped)
```

**Log parquet:**
- `event_type`: `GREEKS_DOWNLOAD_FAILED_INTRADAY`, `GREEKS_CSV_EMPTY_INTRADAY`, `IV_DOWNLOAD_FAILED_INTRADAY`, `IV_CSV_EMPTY_INTRADAY`, `IV_EXCEPTION_INTRADAY`, `ALL_EXPIRATIONS_FAILED`
- `severity`: `WARNING` (per expiration), `ERROR` (per giorno intero)

---

### 3. DataConsistencyLogger (logger.py, linee 191-276)

**Aggiunti due nuovi metodi generici:**

#### `log_error()`
```python
logger.log_error(
    asset="option",
    symbol="AAL",
    interval="5m",
    date="2022-01-03",
    error_type="GREEKS_DOWNLOAD_FAILED",
    error_message="Greeks EOD download returned empty/None - skipping save",
    severity="ERROR",  # or "CRITICAL"
    details={"additional": "context"}  # optional
)
```

#### `log_warning()`
```python
logger.log_warning(
    asset="option",
    symbol="AAL",
    interval="5m",
    date="2022-01-03",
    warning_type="OI_DOWNLOAD_EMPTY",
    warning_message="Open Interest download empty for 20220103",
    details={"additional": "context"}  # optional
)
```

**Caratteristiche:**
- Signature flessibile per logging rapido da qualsiasi punto del codice
- Salvataggio automatico su parquet in `./data/{asset}/{symbol}/{interval}/logs/`
- Campi standard: `timestamp`, `event_type`, `severity`, `symbol`, `asset`, `interval`, `error_message`, `details_json`

---

## Impatto sui Dati Esistenti

### Dati AAL Gennaio 2022 (dal log analizzato)

**Situazione PRIMA delle modifiche:**
- 92 errori HTTP 500 su greeks/IV/OI per AAL
- OHLC salvato, ma greeks/IV/OI **mancanti** (NaN)
- Coherence check **NON rileva** il problema
- Dati **parziali e inutilizzabili** per trading (mancano greeks!)

**Situazione DOPO le modifiche (con re-download):**
- Quando HTTP 500 → **nessun salvataggio** (skip completo)
- Coherence check **rileva** giorni/expirations mancanti:
  - EOD: `MISSING_INTRADAY_DATA` per giorno intero
  - Intraday: `INCOMPLETE_INTRADAY_CANDLES` per expirations mancanti
- Recovery automatico **possibile** (se ThetaData fixa bug server)
- Dati salvati sono **completi** o **assenti** (mai parziali)

---

## Come Testare

### Test Syntax
```bash
cd d:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\tdSynchManager
/c/Users/Federico/anaconda3/python.exe -m py_compile src/tdSynchManager/manager.py
/c/Users/Federico/anaconda3/python.exe -m py_compile src/tdSynchManager/logger.py
```

### Test Logger Methods
```bash
/c/Users/Federico/anaconda3/python.exe test_strict_mode.py
```

### Test EOD Reale (con ThetaData Terminal attivo)
```python
import asyncio
from src.tdSynchManager import ThetaSyncManager

async def test_eod():
    mgr = ThetaSyncManager('thetadata_config.yaml')
    await mgr.run_option_eod_one_day(
        symbol='AAL',
        day_iso='2022-01-03',
        enrich_greeks=True,  # STRICT MODE attivo
        sink='csv'
    )

asyncio.run(test_eod())
```

**Verifica:**
1. Console output deve mostrare `[SKIP-SAVE]` se greeks falliscono
2. File `./data/option/AAL/1d/2022-01-03*` NON deve esistere (skip)
3. Log parquet deve contenere event_type=`GREEKS_DOWNLOAD_FAILED`

### Test Intraday Reale
```python
import asyncio
from src.tdSynchManager import ThetaSyncManager

async def test_intraday():
    mgr = ThetaSyncManager('thetadata_config.yaml')
    await mgr.run_option_intraday_one_day(
        symbol='AAL',
        interval='5m',
        day_iso='2022-01-03',
        enrich_greeks=True,  # STRICT MODE attivo
        sink='csv'
    )

asyncio.run(test_intraday())
```

**Verifica:**
1. Console output deve mostrare `[SKIP-EXPIRATION]` per expirations con errori
2. Se TUTTE le expirations falliscono → `[SKIP-DAY]`
3. File salvati devono contenere SOLO expirations con greeks/IV success
4. Log parquet deve contenere event_type=`GREEKS_DOWNLOAD_FAILED_INTRADAY`

### Verifica Log Parquet
```python
import pandas as pd
import glob

files = glob.glob('./data/option/AAL/*/logs/*.parquet')
for f in files:
    df = pd.read_parquet(f)
    print(df[['timestamp', 'event_type', 'severity', 'error_message']].tail(20))
```

---

## Coherence Check Integration

### Prima delle modifiche
```python
# Coherence check NON rileva greeks mancanti
await checker.check(symbol='AAL', asset='option', interval='5m',
                    start_date='2022-01-03', end_date='2022-01-10')
# Result: is_coherent=True (vede righe OHLC, ignora greeks NaN)
```

### Dopo le modifiche
```python
# Coherence check RILEVA giorni/expirations mancanti
await checker.check(symbol='AAL', asset='option', interval='5m',
                    start_date='2022-01-03', end_date='2022-01-10')
# Result: is_coherent=False
# Issues: [CoherenceIssue(issue_type='MISSING_INTRADAY_DATA', date='2022-01-03'), ...]

# Recovery automatico POSSIBILE (se ThetaData fixa bug)
await recovery.recover(report, fix=True)
# → Re-download dei giorni mancanti
```

---

## Compatibilità Backwards

### Config `enrich_greeks=False`
**Comportamento invariato:**
- Se `enrich_greeks=False` → salva solo OHLC (nessun strict mode)
- Errori su greeks/IV/OI vengono loggati come WARNING ma NON bloccano save

### Config `enrich_greeks=True`
**Nuovo comportamento strict:**
- Salva SOLO se greeks/IV/OI hanno TUTTI successo
- Se qualcuno fallisce → skip save (coherence check detection)

### Migration Path
1. **Fase 1 (ora)**: Run con strict mode su nuovi download
2. **Fase 2**: Coherence check identifica buchi nei dati esistenti
3. **Fase 3**: Recovery automatico quando ThetaData fixa bug server
4. **Fase 4**: Re-download manuale per dati corrotti (se bug persiste)

---

## File Modificati

1. **src/tdSynchManager/manager.py**
   - EOD flow: linee ~1608-1810
   - Intraday flow: linee ~2205-2354
   - Aggiunti log strutturati con `self.logger.log_error()` / `log_warning()`

2. **src/tdSynchManager/logger.py**
   - Aggiunti metodi `log_error()` (linee 191-234)
   - Aggiunti metodi `log_warning()` (linee 236-276)

3. **test_strict_mode.py** (nuovo)
   - Script di test e verifica
   - Esempi di utilizzo
   - Passi per verificare le modifiche

4. **STRICT_MODE_CHANGES.md** (questo file)
   - Documentazione completa delle modifiche

---

## Prossimi Passi

### Immediati
1. ✅ Test syntax (py_compile) - COMPLETATO
2. ✅ Test logger methods - COMPLETATO
3. ⏳ Test EOD reale con AAL 2022-01-03 + ThetaData Terminal
4. ⏳ Test Intraday reale con AAL 2022-01-03 5m
5. ⏳ Verifica log parquet

### Medio Termine
1. **Report a ThetaData Support**
   - Simbolo: AAL
   - Date: 2022-01-03 to 2022-01-10
   - Expirations: 20220916, 20220617, 20220520
   - Error: HTTP 500 con CSV parse error / HTML Jetty error

2. **Coherence Check su Dati Esistenti**
   ```python
   await checker.check(symbol='AAL', asset='option', interval='5m',
                       start_date='2022-01-01', end_date='2022-12-31')
   ```

3. **Recovery Automatico** (dopo fix ThetaData)
   ```python
   await recovery.recover(report, fix=True)
   ```

### Opzionale
4. **Skip List per Expirations Note Broken**
   ```python
   KNOWN_BROKEN_EXPIRATIONS = {
       ('AAL', '20220916'),
       ('AAL', '20220617'),
   }
   if (symbol, exp) in KNOWN_BROKEN_EXPIRATIONS:
       print(f"[SKIP] {symbol} exp={exp} known broken")
       continue
   ```

5. **Column Validation in Coherence Check**
   - Estendere DataValidator per verificare presenza colonne greeks
   - Se `enrich_greeks=True` e colonne greeks mancanti → CoherenceIssue

---

## Domande Frequenti

### Q: I dati esistenti con greeks NaN sono corrotti?
**A**: Sì, sono **inutilizzabili** per trading (mancano delta, gamma, vega, IV, OI). Devono essere ri-scaricati dopo fix ThetaData.

### Q: Coherence check rileva i dati parziali esistenti?
**A**: NO, attualmente rileva solo giorni/candles **completamente mancanti**. Per rilevare greeks mancanti serve estendere il validator (opzionale).

### Q: Posso disabilitare strict mode?
**A**: Sì, imposta `enrich_greeks=False` → comportamento vecchio (salva OHLC anche se greeks falliscono).

### Q: Recovery funziona anche se bug server persiste?
**A**: NO, se HTTP 500 persiste → recovery fallirà di nuovo. Serve fix lato ThetaData.

### Q: Dove trovo i log di skip?
**A**:
- Console: cerca `[SKIP-SAVE]`, `[SKIP-EXPIRATION]`, `[SKIP-DAY]`
- Parquet: `./data/{asset}/{symbol}/{interval}/logs/*.parquet` → event_type colonna

---

## Conclusioni

Le modifiche implementano un **strict mode robusto** che garantisce:

✅ **Coerenza dati**: Solo dati completi (OHLC + greeks/IV/OI) o nessun dato
✅ **Coherence check efficace**: Rileva giorni/expirations mancanti
✅ **Recovery possibile**: Quando ThetaData fixa bug server
✅ **Audit trail completo**: Log strutturati in console + parquet
✅ **Backwards compatible**: `enrich_greeks=False` invariato

❌ **NON risolve**: Bug ThetaData Terminal (HTTP 500) - serve report a loro
❌ **NON rileva**: Greeks NaN nei dati esistenti (serve estensione validator)

---

**Data**: 2025-12-09
**Autore**: Claude Sonnet 4.5
**Versione**: 1.0
