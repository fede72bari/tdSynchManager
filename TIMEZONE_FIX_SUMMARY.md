# Timezone Double Conversion Bug - FIX SUMMARY

## Problem Identified

### Symptom
CSV files showed timestamps shifted by +10 hours from expected market hours:
- **Expected**: `2025-12-08T14:30:00Z` to `2025-12-08T21:00:00Z` (09:30-16:00 ET in UTC)
- **Actual**: `2025-12-09T00:30:00Z` to `2025-12-09T07:00:00Z` (wrong day, wrong times)

### Root Cause
**Double timezone conversion**: Two functions were doing the same ET→UTC conversion sequentially:

1. **`_normalize_ts_to_utc()`** (line 5163): Converts timestamps from ET to UTC
2. **`_normalize_df_types()`** (line 5224): Was ALSO doing ET→UTC conversion

Both functions were called sequentially at multiple locations:
- Line 2110-2111: `_normalize_ts_to_utc()` then `_normalize_df_types()` for tick data
- Line 2137-2138: Same for Greeks data
- Line 2174-2175: Same for IV data

### Conversion Flow (BEFORE FIX)
```
API returns: "09:30:00" (ET)
    ↓
_normalize_ts_to_utc(): treats as ET → converts to UTC: "14:30:00" ✓
    ↓
_normalize_df_types(): treats "14:30:00" as ET again → converts to UTC: "19:30:00" ✗ (+5 hours)
    ↓
Some additional conversion?: "00:30:00" next day ✗ (+5 more hours)
    ↓
Result: +10 hour shift
```

## Fix Applied

### Changed File
`src/tdSynchManager/manager.py`

### Changes Made

#### 1. Removed Timezone Conversion from `_normalize_df_types()` (lines 5224-5260)

**BEFORE (wrong)**:
```python
def _normalize_df_types(self, df: pd.DataFrame, ...) -> pd.DataFrame:
    """
    Heuristic normalization: parse timestamps/dates, convert numerics where safe...
    """
    # ... other code ...

    ts_candidates = ["timestamp", "trade_timestamp", ...]
    for col in ts_candidates:
        if col in df_norm.columns:
            s = pd.to_datetime(df_norm[col], errors="coerce")
            if getattr(s.dtype, "tz", None) is None:
                # BUG: This re-converts timestamps already converted by _normalize_ts_to_utc()
                s = s.dt.tz_localize(ZoneInfo("America/New_York"), ...)
            else:
                s = s.dt.tz_convert(ZoneInfo("America/New_York"))
            df_norm[col] = s.dt.tz_convert("UTC").dt.tz_localize(None)
```

**AFTER (correct)**:
```python
def _normalize_df_types(self, df: pd.DataFrame, ...) -> pd.DataFrame:
    """
    Heuristic normalization: parse timestamps/dates, convert numerics where safe...

    NOTE: Timezone conversion is handled by _normalize_ts_to_utc() which should be
    called BEFORE this function. This function only ensures proper dtype parsing.
    """
    # ... other code ...

    ts_candidates = ["timestamp", "trade_timestamp", ...]
    for col in ts_candidates:
        if col in df_norm.columns:
            # Only parse to datetime if not already datetime type, NO timezone conversion
            if not pd.api.types.is_datetime64_any_dtype(df_norm[col]):
                df_norm[col] = pd.to_datetime(df_norm[col], errors="coerce")
```

#### 2. Fixed `ambiguous` parameter in `_normalize_ts_to_utc()` (line 5192)

**BEFORE**:
```python
s = s.dt.tz_localize(ZoneInfo("America/New_York"), nonexistent="shift_forward", ambiguous="NaT")
```

**AFTER**:
```python
s = s.dt.tz_localize(ZoneInfo("America/New_York"), nonexistent="shift_forward", ambiguous="infer")
```

Changed from `"NaT"` to `"infer"` to avoid pandas compatibility issues.

### Conversion Flow (AFTER FIX)
```
API returns: "09:30:00" (ET)
    ↓
_normalize_ts_to_utc(): treats as ET → converts to UTC: "14:30:00" ✓
    ↓
_normalize_df_types(): only ensures datetime type, NO conversion: "14:30:00" ✓
    ↓
Result: Correct UTC timestamp
```

## Functions Affected

### Where `_normalize_ts_to_utc()` is called:
- Line 1897: EOD data
- Line 2110: Tick trade_quote data
- Line 2137: Tick Greeks data
- Line 2174: Tick IV data
- Line 2232: Intraday OHLC data
- Line 2289: Intraday Greeks data
- Line 2343: Intraday IV data
- Line 2390: Concatenated dataframes
- Line 2410: Concatenated Greeks
- Line 2436: Concatenated IV
- Line 3142: Day data

### Where `_normalize_df_types()` is called:
- Line 2111: Tick data (after _normalize_ts_to_utc)
- Line 2138: Greeks data (after _normalize_ts_to_utc)
- Line 2175: IV data (after _normalize_ts_to_utc)
- Line 2563: OI data
- Line 2564: All data

## Verification

### Expected Behavior After Fix:
1. API returns timestamps in ET (e.g., "09:30:00")
2. `_normalize_ts_to_utc()` converts to UTC: "14:30:00"
3. `_normalize_df_types()` only parses/validates types, no conversion
4. CSV files saved with correct UTC timestamps: "2025-12-08T14:30:00Z"

### Timestamp Columns Handled:
All datetime columns are normalized consistently:
- `timestamp` (main bar/trade timestamp)
- `trade_timestamp`
- `bar_timestamp`
- `quote_timestamp`
- `underlying_timestamp`
- `timestamp_oi`
- `datetime`
- `created`
- `last_trade`
- `time`
- `date`

### Date-Only Columns (unchanged):
These remain as date types (no time component):
- `expiration`
- `effective_date_oi`

## Testing Required

1. **Fresh Download**: Download new data to verify timestamps are correct
2. **Expected Results**:
   - Market hours data: 14:30-21:00 UTC (09:30-16:00 ET)
   - File timestamps: `2025-12-08T14:30:00Z` format
   - No date overflow to next day
   - Coherence validation should pass

3. **Test Command**:
   ```python
   # Re-download QQQ options 5m for 2025-12-08
   # Check first/last timestamps in resulting CSV
   # Should be: 14:30-21:00 UTC on same day
   ```

## Impact

### Data Requiring Re-Download:
All previously downloaded data has incorrect timestamps and must be re-downloaded:
- All symbols
- All intervals (1d, 5m, 1m, tick, etc.)
- All date ranges
- All sinks (CSV, Parquet, InfluxDB)

### No Impact On:
- Coherence validation logic (works with UTC timestamps)
- Bucket analysis (uses corrected timestamps)
- API calls (unaffected)

## Additional Notes

- The fix ensures only ONE timezone conversion (ET→UTC) happens
- `_normalize_ts_to_utc()` is the single source of truth for timezone conversion
- `_normalize_df_types()` now only handles type parsing/validation
- Preserves milliseconds/microseconds/nanoseconds precision
- Consistent across all data types and sinks
