# Docstring Update Status for manager.py

## Summary

- **Total methods/functions**: 112 (including `install_td_server_error_logger` at module level)
- **Updated with complete docstrings**: 21
- **Still need updating**: ~91

## Methods with Complete Docstrings ✅

1. `install_td_server_error_logger` - Module-level function
2. `ThetaSyncManager.__init__` - Constructor
3. `_as_utc` - UTC conversion helper
4. `_floor_to_interval_et` - Interval flooring helper
5. `_iso_date_only` - Date extraction helper
6. `_td_ymd` - ThetaData date format converter
7. `_skip_existing_middle_day` - Resume logic for middle days
8. `run` - Main entry point (PUBLIC)
9. `clear_first_date_cache` - Cache management (PUBLIC)
10. `_validate_interval` - Interval validation
11. `_validate_sink` - Sink validation
12. `_spawn_sync` - Concurrency wrapper
13. `_sync_symbol` - Core synchronization method
14. `_make_file_basepath` - File path construction
15. `_expirations_that_traded` - Option expiration discovery
16. `_fetch_option_all_greeks_by_date` - Greeks data fetching
17. `_write_df_to_sink` - DataFrame persistence router
18. `_cache_key` - Cache key generation
19. `_load_cache_file` - Cache loading
20. `_save_cache_file` - Cache persistence
21. `_touch_cache` - Cache update

## Methods Still Needing Docstring Updates ⚠️

### High Priority - Core Download Methods
- `_download_and_store_options` - Options data download and storage
- `_download_and_store_equity_or_index` - Stock/index data download

### Date/Time Helper Methods
- `_norm_ymd` (nested function in _sync_symbol)
- `_force_start_hms_from_max_ts` (nested function in _sync_symbol)
- `_et_hms_from_iso_utc` - ET time extraction
- `_parse_date` - Date parsing
- `_iter_days` - Date iteration

### Discovery Methods
- `_resolve_first_date` - First date resolution (has incomplete docstring)
- `_discover_equity_first_date` - Equity first date discovery
- `_discover_option_first_date` - Option first date discovery
- `_binary_search_first_date_option` - Binary search for options
- `has_data_async` (nested function)

### File Operations
- `_read_minimal` (nested function) - Minimal DataFrame reading
- `_max_file_ts` (nested function) - Max timestamp extraction
- `_list_series_files` - Series file listing
- `_series_earliest_and_latest_day` - Series date range
- `_start_from_filename` (nested function) - Date extraction from filename
- `_get_first_last_day_from_sink` - Sink-specific date range
- `_list_day_files` - Daily file listing
- `_list_day_part_files` - Part file listing for a day
- `_day_parts_status` - Part file status checking
- `_purge_day_files` - File cleanup

### Data Processing
- `_norm8` (nested function) - 8-digit date normalization
- `_dig_to_list` (nested function) - Data structure normalization
- `_extract_days_from_df` - DataFrame date extraction
- `_min_ts_from_df` - Min timestamp from DataFrame
- `_max_ts_from_file` (nested function) - Max timestamp from file
- `_parse_csv_first_col_as_dt` - CSV timestamp parsing
- `_first_timestamp_in_csv` - First timestamp extraction (CSV)
- `_first_timestamp_in_parquet` - First timestamp extraction (Parquet)

### Parquet Operations
- `_append_parquet_df` - Parquet append with dedup
- `_part_num_from_path` (nested function) - Part number extraction
- `_make_target` (nested function) - Target path generation
- `_should_stop` (nested function) - Rotation check
- `_write_one_chunk` (nested function) - Chunk writing
- `_write_parquet_from_csv` - CSV to Parquet conversion

### CSV Operations
- `_append_csv_text` - CSV text appending
- `_tail_csv_last_n_lines` - Tail CSV file
- `_last_csv_day` - Last day in CSV
- `_last_csv_timestamp` - Last timestamp in CSV
- `_csv_has_day` - Check if CSV has data for day
- `_tail_one_line` - Tail single line

### InfluxDB Operations
- `_ensure_influx_client` - InfluxDB client initialization
- `_influx_measurement_from_base` - Measurement name generation
- `_influx_last_timestamp` - Last timestamp query
- `_influx__et_day_bounds_to_utc` - Day bounds conversion
- `_influx__first_ts_between` - First timestamp in range
- `_influx_day_has_any` - Check if day has data
- `_influx_first_ts_for_et_day` - First timestamp for day
- `_append_influx_df` - DataFrame append to InfluxDB
- `_esc` (nested function) - Escape special characters
- `_to_ns` (nested function) - Timestamp to nanoseconds
- `_influx_last_ts_between` - Last timestamp between dates

### Resume Logic
- `_compute_resume_start_datetime` - Resume start calculation
- `_compute_intraday_window_et` - Intraday window calculation (2 definitions - check for duplicate)
- `_probe_existing_last_ts_with_source` - Last timestamp probing
- `_debug_log_resume` - Resume debug logging

### Cache & Metadata
- `_get_cached_first_date` - Get cached first date
- `_set_cached_first_date` - Set cached first date

### File Management
- `_next_part_path` - Next part file path
- `_pick_latest_part` - Latest part selection
- `_ensure_under_cap` - File size capping
- `_find_existing_series_base` - Find series base path
- `_find_existing_daily_base_for_day` - Find daily base path
- `_sink_dir_name` - Sink directory name
- `_iso_stamp` - ISO timestamp generation
- `_detect_time_col` - Detect time column name

### Utility Methods
- `_ensure_list` - Ensure data is list format
- `_normalize_date_str` - Date string normalization
- `_extract_first_date_from_any` - Extract first date from sequence
- `_extract_expirations_as_dates` - Extract expirations
- `_maybe_await` - Conditional await
- `_read_minimal_frame` - Minimal frame reading
- `_max_file_timestamp` - Max file timestamp
- `_td_get_with_retry` - API call with retry

### Gap Detection
- `_missing_1d_days_csv` - Find missing daily data

### Analysis/Screening Methods (PUBLIC)
- `screen_option_oi_concentration` - OI concentration screening
- `_fetch_oi` (nested function)
- `screen_option_volume_concentration` - Volume concentration screening
- `_fetch_eod` (nested function)

### Duplicate Checking (PUBLIC)
- `duplication_and_strike_checks` - Comprehensive duplicate checking
- `_key_cols` (nested function)
- `_read_minimal` (nested function in duplication_and_strike_checks)
- `check_duplicates_in_sink` - Sink-specific duplicate checking
- `_check_duplicates_influx` - InfluxDB duplicate checking
- `_check_duplicates_file` - File duplicate checking
- `check_duplicates_multi_day` - Multi-day duplicate checking

## Required Docstring Structure

Each method needs:

```python
"""[INTRODUCTION - 1-2 paragraphs]
Narrative description of what the function/method does and what it returns.

Parameters
----------
param_name : param_type, optional (if applicable)
    Default: value (if applicable)
    Possible values: [list if constrained]

    Detailed explanation of what the parameter means and how it affects behavior.

Returns
-------
return_type
    Description of what is returned.

Example Usage
-------------
# Concrete example showing usage
# For internal/helper methods (starting with _), mention which methods call them
```

## Notes

1. **Internal methods** (starting with `_`) should mention in their Example Usage which methods call them
2. **Nested functions** (defined inside other methods) also need docstrings
3. **Default values** must be clearly specified
4. **Constrained parameters** should list all possible values
5. **NO code modification** - only docstrings

## Progress Tracking

- Started: 2025-11-13
- Methods updated so far: 21 / 112 (18.75%)
- Methods remaining: 91 / 112 (81.25%)

## Completion Strategy

Due to the large number of methods, consider:
1. Prioritize PUBLIC methods (no underscore prefix)
2. Then core download/sync methods
3. Then file operations
4. Then utility helpers
5. Finally nested functions

The file is approximately 5000+ lines, so systematic batch updates are recommended.
