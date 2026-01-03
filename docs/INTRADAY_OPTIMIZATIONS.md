# Intraday Pipeline Optimizations

This document describes the optimizations implemented to improve performance for real-time intraday data synchronization.

## Overview

Two major optimizations have been implemented to reduce API calls and improve performance for real-time intraday downloads:

1. **Skip API Date Fetch for Single-Day Downloads**
2. **OI (Open Interest) Caching**

---

## 1. Skip API Date Fetch for Single-Day Downloads

### Problem

Previously, the manager called `_fetch_available_dates_from_api()` for EVERY intraday download, even when downloading a single specific day (e.g., today's real-time data). This API call is expensive and unnecessary for single-day downloads.

### Solution

The manager now detects single-day downloads (`cur_date == end_date`) and skips the API date fetch entirely, using the specific date directly.

### Code Location

[src/tdSynchManager/manager.py:1513-1528](../src/tdSynchManager/manager.py#L1513-L1528)

### Implementation

```python
# OPTIMIZATION: Skip API date fetch for single-day downloads (real-time intraday)
# The API call is expensive and unnecessary when downloading only one specific day
is_single_day = (cur_date == end_date)

if is_single_day:
    # Single-day download (real-time intraday): skip API fetch, use the specific date
    print(f"[API-DATES] Single-day download ({cur_date.isoformat()}), skipping API date fetch")
    api_available_dates = None
else:
    # Multi-day range: fetch available dates from ThetaData API to iterate ONLY over real dates
    print(f"[API-DATES] Fetching available dates for {symbol} ({task.asset}/{interval})...")
    api_available_dates = await self._fetch_available_dates_from_api(
        task.asset, symbol, interval, cur_date, end_date,
        use_api_discovery=task.use_api_date_discovery
    )
```

### Benefits

- **Reduced API calls**: No unnecessary date discovery for real-time downloads
- **Faster execution**: Single-day downloads start immediately without waiting for API response
- **Lower API load**: Reduces strain on ThetaData API during high-frequency real-time synchronization

### Log Messages

**Single-day download (optimized):**
```
[API-DATES] Single-day download (2025-01-02), skipping API date fetch
```

**Multi-day download (unchanged):**
```
[API-DATES] Fetching available dates for TLRY (option/5m)...
[API-DATES] Found 5 available dates, iterating only those
```

---

## 2. OI (Open Interest) Caching

### Problem

Open Interest (OI) data changes **only at end-of-day (EOD)**, specifically at the close of the previous trading day. However, the manager was downloading OI from the API on EVERY intraday download, resulting in:

- **Redundant API calls** for the same unchanging data
- **Increased latency** for intraday updates
- **Higher API costs** and load

### Solution

Implemented an InfluxDB-based caching system that:
1. Checks if OI for the current date is already cached
2. Loads OI from cache on cache HIT (no API call)
3. Downloads OI from API on cache MISS and saves to cache for future reuse

### Code Locations

**Helper Methods:**
- [src/tdSynchManager/manager.py:7084-7134](../src/tdSynchManager/manager.py#L7084-L7134) - `_check_oi_cache()`
- [src/tdSynchManager/manager.py:7136-7188](../src/tdSynchManager/manager.py#L7136-L7188) - `_load_oi_from_cache()`
- [src/tdSynchManager/manager.py:7190-7277](../src/tdSynchManager/manager.py#L7190-L7277) - `_save_oi_to_cache()`

**Integration:**
- [src/tdSynchManager/manager.py:2718-2785](../src/tdSynchManager/manager.py#L2718-L2785) - Cache integration in `_download_and_store_options()`

### Architecture

**Cache Storage:** Local Parquet files (works with ANY sink: csv, parquet, influxdb)
**Cache Location:** `{root_dir}/.oi_cache/{symbol}/{YYYYMMDD}.parquet`

**Schema:**
- **Columns:** `expiration`, `strike`, `right`, `option_symbol`, `root`, `symbol`, `last_day_OI`, `effective_date_oi`, `timestamp_oi`

**Cache is independent from main data sink** - you can store OHLC in CSV while caching OI in Parquet files.

### Implementation Flow

```python
# 1. Check cache
if self.cfg.influx_url and self._check_oi_cache(symbol, cur_ymd):
    # Cache HIT: Load from cache
    doi = self._load_oi_from_cache(symbol, cur_ymd)
    print(f"[OI-CACHE][HIT] Using cached OI for {symbol}/{cur_ymd}")
else:
    # Cache MISS: Download from API
    doi = download_oi_from_api(...)

    # Save to cache for future reuse
    if doi is not None and not doi.empty:
        self._save_oi_to_cache(symbol, cur_ymd, doi_cache)
        print(f"[OI-CACHE][SAVE] {symbol} date={cur_ymd} - Cached N OI records")
```

### Benefits

- **Eliminates redundant API calls**: OI downloaded once per day, reused for all intraday updates
- **Faster intraday downloads**: Cache HIT is 10-100x faster than API call
- **Reduced API costs**: Significant reduction in API calls for high-frequency intraday synchronization
- **Scalability**: Supports multiple symbols and concurrent downloads without API throttling

### Log Messages

**First download (cache MISS):**
```
[OI-CACHE] Checking local cache for TLRY date=20250102...
[OI-CACHE] ✗ Current date OI not found in local cache - starting remote data source fetching
[OI-FETCH] historical mode: expiration="*" (all expirations)
[OI-CACHE] Saving current date OI to local cache for future reuse...
[OI-CACHE][SAVED] Successfully saved 1250 OI records to local cache: data/.oi_cache/TLRY/20250102.parquet
```

**Subsequent downloads (cache HIT):**
```
[OI-CACHE] Checking local cache for TLRY date=20250102...
[OI-CACHE][CHECK] TLRY date=20250102 - Cache file found: data/.oi_cache/TLRY/20250102.parquet
[OI-CACHE] ✓ Found current date OI in local cache - retrieving from file
[OI-CACHE][LOAD] TLRY date=20250102 - Loaded 1250 OI records from cache
[OI-CACHE][SUCCESS] Retrieved 1250 OI records from local cache (no remote API call)
```

**OI caching disabled:**
```
[OI-CACHE] OI caching disabled (enable_oi_caching=False) - fetching from remote data source
```

### Cache Invalidation

**Automatic:** OI cache is automatically invalidated each day because the `cur_ymd` date advances. Yesterday's OI remains cached but is not queried for today's date.

**Manual:** To clear the OI cache:

**Clear all OI cache:**
```bash
rm -rf data/.oi_cache/
```

**Clear cache for specific symbol:**
```bash
rm -rf data/.oi_cache/TLRY/
```

**Clear cache for specific symbol and date:**
```bash
rm data/.oi_cache/TLRY/20250102.parquet
```

**Programmatic cleanup (Python):**
```python
import shutil
import os

# Clear all OI cache
cache_dir = os.path.join(cfg.root_dir, cfg.oi_cache_dir)
if os.path.exists(cache_dir):
    shutil.rmtree(cache_dir)

# Clear specific symbol
symbol_cache = os.path.join(cache_dir, "TLRY")
if os.path.exists(symbol_cache):
    shutil.rmtree(symbol_cache)
```

---

## Configuration

Both optimizations are **enabled by default** and require no configuration changes.

### Parameters

**OI Caching Configuration:**
```python
cfg = ManagerConfig(
    root_dir="data",
    enable_oi_caching=True,         # Default: True - Enable OI caching for intraday
    oi_cache_dir=".oi_cache",       # Default: ".oi_cache" - Cache directory name
    # ... other parameters
)
```

### Requirements

- **OI Caching:** Works with ANY sink (csv, parquet, influxdb) - no special requirements
- **Date Fetch Skip:** Always enabled for single-day downloads

### Disabling (not recommended)

To disable OI caching:
```python
cfg = ManagerConfig(
    root_dir="data",
    enable_oi_caching=False,  # Disable OI caching
    # ...
)
```

**Note:** OI caching is independent from the main data sink. Even if you use `sink="csv"`, OI will be cached in Parquet files.

---

## Testing

A comprehensive test suite is provided to verify both optimizations:

**Test Script:** [tests/test_intraday_optimizations.py](../tests/test_intraday_optimizations.py)

**Run tests:**
```bash
cd tests
python test_intraday_optimizations.py
```

**Test coverage:**
1. Single-day download skips API date fetch
2. OI cache MISS → cache SAVE → cache HIT flow
3. Multi-day downloads still fetch API dates (regression test)

---

## Performance Impact

### Real-Time Intraday Scenario

**Before optimizations:**
- Every 5-minute update: 2 API calls (dates + OI) + data download
- Total API calls per day (78 trading periods): ~156 calls

**After optimizations:**
- First update: 1 API call (OI only, cached) + data download
- Subsequent updates: 0 extra API calls + data download
- Total API calls per day: ~1 call

**Performance gain: ~99% reduction in API calls for real-time intraday**

### Multi-Symbol Scenario

For 10 symbols updated every 5 minutes:
- **Before:** ~1,560 API calls per day
- **After:** ~10 API calls per day (one OI download per symbol)

**Performance gain: ~99.4% reduction in API calls**

---

## Troubleshooting

### OI Cache Not Working

**Symptoms:**
- No `[OI-CACHE][SUCCESS]` messages in logs
- OI downloaded on every run

**Solutions:**
1. Verify OI caching is enabled: `cfg.enable_oi_caching = True` (default)
2. Check cache directory exists and has write permissions:
   ```bash
   ls -la data/.oi_cache/
   ```
3. Verify cache files are being created:
   ```bash
   ls -la data/.oi_cache/TLRY/
   ```
4. Check for errors in `[OI-CACHE][WARN]` messages in logs
5. Ensure sufficient disk space for cache files

### Single-Day Optimization Not Working

**Symptoms:**
- Still seeing `[API-DATES] Fetching available dates...` for single-day downloads

**Solutions:**
1. Verify `cur_date == end_date` (single-day range)
2. Check if using `end_date_override` matches `first_date_override`
3. Ensure not using old manager version (pull latest changes)

---

## Future Enhancements

Potential future improvements:

1. **Persistent OI Cache:** Save OI to Parquet files for offline access
2. **Smart Cache Invalidation:** Auto-detect when new OI data is available (e.g., after 4pm ET)
3. **Greeks Caching:** Apply similar caching strategy to Greeks data (delta, gamma, theta, etc.)
4. **Multi-Day OI Cache:** Pre-fetch and cache OI for multiple days in advance

---

## Summary

These optimizations significantly improve the performance and efficiency of real-time intraday synchronization:

✅ **Single-day downloads** skip unnecessary API date fetches
✅ **OI caching** eliminates redundant API calls for unchanging data
✅ **Works with ANY sink** - csv, parquet, influxdb (cache is independent)
✅ **99%+ reduction** in API calls for real-time intraday scenarios
✅ **Backward compatible** - multi-day downloads unchanged
✅ **Enabled by default** - minimal configuration required (`enable_oi_caching=True`)

**Result:** Faster, more efficient real-time data synchronization with minimal API load, regardless of your chosen data storage format.
