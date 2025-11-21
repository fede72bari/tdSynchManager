# Discovery Behavior Analysis - ThetaData Synchronization Manager

## Overview

This document analyzes how the `discover_policy.mode` setting affects the update/synchronization behavior across different timeframes (TF), sinks, and asset types in the ThetaData Synchronization Manager.

## Discovery Policy Modes

The `DiscoverPolicy` class in `config.py` defines the following modes:

- **`"auto"`** (default): Uses cached first-date if available, otherwise performs discovery
- **`"force"`**: Ignores cache and always re-discovers (not fully implemented in current version)
- **`"mild_skip"`**: Skips global discovery but keeps per-day middle checks (legacy behavior)
- **`"skip"`**: Strong skip - no global discovery AND bypasses per-day middle checks

## Discovery Behavior by Asset Type

### 1. STOCKS (`asset="stock"`)

#### Discovery Method: `_discover_equity_first_date()`
- **Endpoint Used**: `/stock/list/dates`
- **Parameters**:
  - `data_type`: "trade" or "quote" (from `discover_policy.request_type`)
  - `symbol`: Stock ticker (e.g., "AAPL")

#### Behavior Differences:

**Mode: `"auto"` or `"force"`**
```python
# Checks cache first (if mode="auto")
cached = self._get_cached_first_date(asset="stock", symbol="AAPL", req_type="trade")

# If not cached (or mode="force"), calls discovery
data, _ = await client.stock_list_dates(
    symbol="AAPL",
    data_type="trade",  # or "quote"
    format_type="json"
)
# Returns earliest date like "2010-01-04"
```

**Mode: `"skip"` or `"mild_skip"`**
```python
# Returns None immediately - no discovery performed
# Relies on:
#   1. task.first_date_override (if set)
#   2. Resume from existing data in sink
#   3. Falls back to current date if nothing found
```

**Impact on Different Sinks**:
- **CSV/Parquet**: Discovery is sink-independent. The first date is determined from ThetaData API, then data is written to the sink.
- **InfluxDB**: Same - discovery doesn't depend on sink type.

**Impact on Different Timeframes**:
- **Same for all TF**: Discovery finds the first available date for the stock, regardless of whether you're downloading "1m", "5m", "1h", or "1d" data. The timeframe affects the download phase, not discovery.

---

### 2. INDICES (`asset="index"`)

#### Discovery Method: `_discover_equity_first_date()` (same as stocks but different endpoint)
- **Endpoint Used**: `/index/list/dates`
- **Parameters**:
  - `data_type`: "price" or "ohlc" (automatically mapped from "trade"/"quote")
  - `symbol`: Index symbol (e.g., "$SPX", "$VIX")

#### Behavior Differences:

**Automatic Mapping**:
```python
# If req_type is "trade" or "quote" → maps to "price" for indices
# Indices don't have trade/quote semantics like stocks

if asset == "index":
    if req_type in ("price", "ohlc"):
        idx_type = req_type
    else:
        idx_type = "price"  # Default mapping
```

**Mode Behavior**: Same as stocks
- `"auto"`: Checks cache → calls API if needed
- `"skip"`/`"mild_skip"`: No discovery, uses overrides/resume

**Impact on Sinks & Timeframes**: Same as stocks - discovery is independent of sink and TF.

---

### 3. OPTIONS (`asset="option"`)

#### Discovery Method: `_discover_option_first_date()` (most complex)
- **Endpoints Used**:
  1. `/option/list/expirations` - Get all expiration dates
  2. `/option/list/dates/{req_type}` - Check dates for each expiration
  3. `/option/list/contracts/{req_type}` - Fallback binary search

#### Multi-Strategy Approach:

**Strategy 1: Expiration-Based Discovery**
```python
# Step 1: Get all expirations
exps, _ = await client.option_list_expirations(symbol="AAPL", format_type="json")
exp_dates = sorted(expirations)  # e.g., ["2010-01-15", "2010-02-19", ...]

# Step 2: Check first K expirations (default: 3)
check_first_k = discover_policy.check_first_k_expirations or cfg.option_check_first_k_expirations

for exp in exp_dates[:check_first_k]:
    payload, _ = await client.option_list_dates(
        symbol="AAPL",
        request_type="trade",  # or "quote"
        expiration=exp,
        strike="*",      # All strikes
        right="both",    # Calls and puts
        format_type="json"
    )
    # Extract earliest date from this expiration

# Return minimum date across all checked expirations
```

**Strategy 2: Binary Search Fallback**
```python
# If Strategy 1 finds nothing AND fallback_binary=True
if discover_policy.enable_option_binary_fallback:
    first_date = await _binary_search_first_date_option(
        symbol="AAPL",
        req_type="trade",
        start_date=discover_policy.binary_search_start,  # Default: "2010-01-01"
        end_date=discover_policy.binary_search_end       # Default: today
    )
```

#### Option-Specific Parameters:

**`discover_policy.check_first_k_expirations`**:
- **Default**: 3
- **Effect**: Number of oldest expirations to check
- **Trade-off**: More checks = more reliable but more API calls
- Example: `check_first_k_expirations=5` checks 5 oldest expirations

**`discover_policy.enable_option_binary_fallback`**:
- **Default**: True
- **Effect**: Enables binary search if expiration-based discovery fails
- Useful for symbols with limited expiration metadata

**`discover_policy.binary_search_start/end`**:
- **Default**: "2010-01-01" to today
- **Effect**: Date range for binary search fallback

#### Mode Behavior for Options:

**Mode: `"auto"`**
```python
# 1. Check cache
cached = self._get_cached_first_date(asset="option", symbol="AAPL", req_type="trade")
if cached:
    return cached

# 2. Perform expiration-based discovery (Strategy 1)
first_date = await _discover_option_first_date(...)

# 3. Cache result
self._set_cached_first_date(asset="option", symbol="AAPL", req_type="trade", first_date)
```

**Mode: `"force"`** (not fully implemented):
```python
# Intended behavior: Skip cache, always re-discover
# Current implementation treats it like "auto" without cache check
```

**Mode: `"skip"` or `"mild_skip"`**:
```python
# No discovery
# Uses task.first_date_override or resume from sink
```

**Impact on Different Timeframes**:
- **Same for all TF**: Options discovery finds the first date WITH option data, regardless of TF.
- Exception: Resume logic may differ - see below.

**Impact on Different Sinks**: Same as stocks - sink-independent.

---

## Resume Behavior with Discovery

### How Discovery Affects Resume

When `discover_policy.mode != "skip"`, the discovered first date is used as the **initial coverage date** for a symbol. The resume logic then:

1. **First Run** (no existing data):
   - Uses discovered first date as start
   - Downloads from first_date to current date

2. **Subsequent Runs** (data exists):
   - Reads last saved timestamp from sink
   - Resumes from `last_timestamp - overlap_seconds`
   - Discovery is skipped (uses cached value)

### Per-Day Middle Checks

**`discover_policy.mode = "mild_skip"`**:
- Skips **global discovery** (finding first date)
- BUT keeps **per-day middle checks**:
  ```python
  # In download logic, checks if middle days already exist:
  if _skip_existing_middle_day(day_iso=..., first_last_hint=...):
      print(f"Skipping {day_iso} - already exists and is a middle day")
      continue
  ```

**`discover_policy.mode = "skip"`** (strong skip):
- Skips both global discovery AND per-day middle checks
- Always attempts to download/update every day in range

---

## Sink-Specific Differences

### Discovery Phase (Finding First Date)
**No differences** - Discovery queries ThetaData API only, doesn't depend on sink.

### Download/Resume Phase

#### CSV Sink
```python
# Resume: Read last line of CSV file
last_lines = _tail_csv_last_n_lines(path, n=64)
last_timestamp = _parse_csv_first_col_as_dt(last_lines[0])
resume_from = last_timestamp - timedelta(seconds=overlap_seconds)
```

#### Parquet Sink
```python
# Resume: Read Parquet file, get max timestamp
df = pd.read_parquet(path)
last_timestamp = df['timestamp'].max()
resume_from = last_timestamp - pd.Timedelta(seconds=overlap_seconds)
```

#### InfluxDB Sink
```python
# Resume: Query InfluxDB for max timestamp
query = f'''
    SELECT MAX(time) FROM "{measurement}"
    WHERE symbol = '{symbol}' AND interval = '{interval}'
'''
last_timestamp = influx_client.query(query)
resume_from = last_timestamp - timedelta(seconds=overlap_seconds)
```

**Key Difference**: InfluxDB queries can be slower for large datasets, but resume logic is otherwise identical.

---

## Timeframe-Specific Differences

### Discovery Phase
**No differences** - All timeframes use the same first date discovery.

### Download Phase

#### Daily Data (`interval="1d"`)
```python
# Uses /stock/history/eod or /option/history/eod
# Resume: Jump to next day
resume_from_day = last_saved_day + timedelta(days=1)
```

#### Intraday Data (`interval="1m", "5m", etc.`)
```python
# Uses /stock/history/ohlc or /option/history/ohlc
# Resume: Uses overlap_seconds (default: 60)
resume_from = last_timestamp - timedelta(seconds=60)
```

#### Tick Data (`interval="tick"`)
```python
# Uses /stock/history/trade or /option/history/trade
# Resume: Uses overlap_seconds, typically higher (e.g., 300)
resume_from = last_timestamp - timedelta(seconds=300)
```

**Trade-off**: Higher overlap for tick data reduces risk of missing ticks but increases duplicate potential.

---

## Summary Table

| Aspect | Stock/Index | Options | Notes |
|--------|-------------|---------|-------|
| **Discovery Endpoint** | `/stock/list/dates` or `/index/list/dates` | `/option/list/expirations` + `/option/list/dates` + optional binary search | Options more complex |
| **Cache Used** | Yes (when mode="auto") | Yes (when mode="auto") | Shared cache mechanism |
| **Binary Search Fallback** | No | Yes (configurable) | Options only |
| **Check First K** | N/A | Configurable (default: 3) | Options only |
| **Sink Impact on Discovery** | None | None | Discovery is sink-independent |
| **TF Impact on Discovery** | None | None | Discovery is TF-independent |
| **Resume Overlap** | 60s (default) | 60s (default) | Configurable per task |
| **Per-Day Middle Checks** | Available in "mild_skip" | Available in "mild_skip" | Prevents re-downloading existing middle days |

---

## Configuration Examples

### Example 1: Auto Discovery (Recommended)
```python
Task(
    asset="option",
    symbols=["AAPL", "SPY"],
    intervals=["5m", "1h"],
    sink="influxdb",
    discover_policy=DiscoverPolicy(
        mode="auto",              # Use cache, discover if missing
        request_type="trade",     # Check trade data availability
        check_first_k_expirations=3,  # Check 3 oldest expirations
        enable_option_binary_fallback=True  # Enable fallback
    )
)
```

### Example 2: Force Re-Discovery (Cache Invalidation)
```python
# Clear cache manually first
manager.clear_first_date_cache(asset="option", symbol="AAPL", req_type="trade")

Task(
    asset="option",
    symbols=["AAPL"],
    intervals=["1m"],
    sink="csv",
    discover_policy=DiscoverPolicy(
        mode="auto",  # Will discover since cache is cleared
    )
)
```

### Example 3: Skip Discovery (Explicit Date)
```python
Task(
    asset="stock",
    symbols=["AAPL"],
    intervals=["1d"],
    sink="parquet",
    first_date_override="2020-01-01",  # Explicit start date
    discover_policy=DiscoverPolicy(
        mode="skip"  # No discovery, use override
    )
)
```

### Example 4: Mild Skip (Resume-Friendly)
```python
Task(
    asset="option",
    symbols=["TLRY"],
    intervals=["tick", "5m"],
    sink="influxdb",
    discover_policy=DiscoverPolicy(
        mode="mild_skip"  # Skip discovery, keep per-day checks
    )
)
# Use case: Daily incremental updates, avoid re-checking first date
```

---

## Recommendations

1. **First-Time Downloads**: Use `mode="auto"` with `enable_option_binary_fallback=True`
2. **Daily Updates**: Use `mode="mild_skip"` to skip discovery but preserve middle-day checks
3. **Historical Backfills**: Use explicit `first_date_override` with `mode="skip"`
4. **Multiple Timeframes**: Discovery date is shared - all TF start from same first date
5. **Multiple Sinks**: Run discovery once, result is cached regardless of sink

---

## Potential Issues & Solutions

### Issue 1: HTTP 414 (URI Too Long)
**Cause**: Requesting too many option strikes/expirations in one call
**Solution**: Current code queries ONE expiration at a time (already fixed)

### Issue 2: Slow Option Discovery
**Cause**: Checking many expirations
**Solution**: Reduce `check_first_k_expirations` (e.g., from 3 to 1)

### Issue 3: Cached Wrong Date
**Cause**: First discovery returned incorrect date
**Solution**: Clear cache and re-run with `mode="auto"`
```python
manager.clear_first_date_cache(asset="option", symbol="AAPL", req_type="trade")
```

### Issue 4: Duplicates from Multiple Runs
**Cause**: Running same task multiple times without `ignore_existing=True`
**Solution**: Set `ignore_existing=False` (default) to enable resume logic

---

## Conclusion

**Key Takeaway**: Discovery behavior depends primarily on `discover_policy.mode` and `asset` type, NOT on sink or timeframe. Options have the most sophisticated discovery with multiple fallback strategies.

**Recommended Workflow**:
1. **Initial Setup**: Use `mode="auto"` to discover and cache first dates
2. **Daily Updates**: Switch to `mode="mild_skip"` for faster execution
3. **Custom Backfills**: Use `first_date_override` with `mode="skip"`
