import re

input_file = r'D:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\repo-claude\src\tdSynchManager\manager.py'
output_file = r'D:\Dropbox\TRADING\DATA FEEDERS AND APIS\ThetaData\repo-claude\src\tdSynchManager\manager_reorganized.py'

with open(input_file, 'r', encoding='utf-8') as f:
    lines = f.readlines()

class_pattern = re.compile(r'^class ThetaSyncManager:')
method_pattern = re.compile(r'^    (async )?def ([a-zA-Z_][a-zA-Z0-9_]*)\(')

class_start = None
for i, line in enumerate(lines):
    if class_pattern.match(line):
        class_start = i
        break

preamble_lines = lines[:class_start + 1]

first_method_line = None
in_docstring = False
for i in range(class_start + 1, len(lines)):
    line = lines[i]
    if '"""' in line:
        if not in_docstring:
            in_docstring = True
            if line.count('"""') == 2:
                in_docstring = False
        else:
            in_docstring = False
        continue
    if not in_docstring and method_pattern.match(line):
        first_method_line = i
        break

preamble_lines.extend(lines[class_start + 1:first_method_line])

method_starts = []
for i in range(first_method_line, len(lines)):
    if method_pattern.match(lines[i]):
        match = method_pattern.match(lines[i])
        method_name = match.group(2)
        method_starts.append((i, method_name))

method_dict = {}
for idx, (start_line, method_name) in enumerate(method_starts):
    if idx + 1 < len(method_starts):
        end_line = method_starts[idx + 1][0]
    else:
        end_line = len(lines)
    method_lines = lines[start_line:end_line]
    method_dict[method_name] = method_lines

print(f'Extracted {len(method_dict)} methods')

groups = [
    ('INITIALIZATION', {'public': ['__init__'], 'helper': []}),
    ('DATA SYNCHRONIZATION', {'public': ['run'], 'helper': ['_spawn_sync', '_sync_symbol', '_download_and_store_options', '_download_and_store_equity_or_index', '_expirations_that_traded', '_fetch_option_all_greeks_by_date', '_td_get_with_retry', '_resolve_first_date', '_discover_equity_first_date', '_discover_option_first_date', '_binary_search_first_date_option', '_extract_first_date_from_any', '_extract_expirations_as_dates', '_compute_resume_start_datetime', '_skip_existing_middle_day', '_get_first_last_day_from_sink', '_probe_existing_last_ts_with_source', '_missing_1d_days_csv', '_debug_log_resume', '_compute_intraday_window_et', '_write_df_to_sink', '_append_csv_text', '_append_parquet_df', '_write_parquet_from_csv', '_append_influx_df', '_make_file_basepath', '_list_series_files', '_series_earliest_and_latest_day', '_list_day_files', '_list_day_part_files', '_day_parts_status', '_find_existing_series_base', '_find_existing_daily_base_for_day', '_pick_latest_part', '_next_part_path', '_ensure_under_cap', '_purge_day_files', '_sink_dir_name', '_csv_has_day', '_last_csv_day', '_last_csv_timestamp', '_first_timestamp_in_csv', '_first_timestamp_in_parquet', '_max_file_timestamp', '_read_minimal_frame', '_ensure_influx_client', '_influx_measurement_from_base', '_influx_last_timestamp', '_influx__et_day_bounds_to_utc', '_influx__first_ts_between', '_influx_day_has_any', '_influx_first_ts_for_et_day', '_influx_last_ts_between', '_get_cached_first_date', '_set_cached_first_date', '_load_cache_file', '_save_cache_file', '_touch_cache', '_cache_key']}),
    ('MARKET SCREENING', {'public': ['screen_option_oi_concentration', 'screen_option_volume_concentration'], 'helper': []}),
    ('DATA QUALITY VALIDATION', {'public': ['generate_duplicate_report', 'duplication_and_strike_checks', 'check_duplicates_in_sink', 'check_duplicates_multi_day'], 'helper': ['_check_duplicates_influx', '_check_duplicates_file']}),
    ('LOCAL DB QUERY', {'public': [], 'helper': []}),
    ('COMMON UTILITIES', {'public': ['clear_first_date_cache'], 'helper': ['_validate_interval', '_validate_sink', '_as_utc', '_floor_to_interval_et', '_iso_date_only', '_td_ymd', '_iso_stamp', '_et_hms_from_iso_utc', '_min_ts_from_df', '_ensure_list', '_normalize_date_str', '_parse_date', '_iter_days', '_tail_csv_last_n_lines', '_tail_one_line', '_parse_csv_first_col_as_dt', '_extract_days_from_df', '_detect_time_col', '_maybe_await']})
]

output_lines = []
output_lines.extend(preamble_lines)

added_methods = set()
missing_methods = []

for group_name, group_methods in groups:
    output_lines.append('\n')
    output_lines.append('    # =========================================================================\n')
    output_lines.append('    # (BEGIN)\n')
    output_lines.append(f'    # {group_name}\n')
    output_lines.append('    # =========================================================================\n')

    for method_name in group_methods['public']:
        if method_name in method_dict:
            output_lines.extend(method_dict[method_name])
            added_methods.add(method_name)
        else:
            missing_methods.append(method_name)
            print(f'WARNING: Method {method_name} not found')

    for method_name in group_methods['helper']:
        if method_name in method_dict:
            output_lines.extend(method_dict[method_name])
            added_methods.add(method_name)
        else:
            missing_methods.append(method_name)
            print(f'WARNING: Method {method_name} not found')

    output_lines.append('\n')
    output_lines.append('    # =========================================================================\n')
    output_lines.append('    # (END)\n')
    output_lines.append(f'    # {group_name}\n')
    output_lines.append('    # =========================================================================\n')

all_expected = set()
for _, group_methods in groups:
    all_expected.update(group_methods['public'])
    all_expected.update(group_methods['helper'])

not_categorized = set(method_dict.keys()) - all_expected
if not_categorized:
    print(f'\nWARNING: Not categorized methods:')
    for method in sorted(not_categorized):
        print(f'  - {method}')

with open(output_file, 'w', encoding='utf-8') as f:
    f.writelines(output_lines)

print(f'\nReorganization complete!')
print(f'Total methods in source: {len(method_dict)}')
print(f'Total methods added: {len(added_methods)}')
print(f'Missing methods: {len(missing_methods)}')
print(f'Not categorized: {len(not_categorized)}')
print(f'\nOutput written to: {output_file}')
