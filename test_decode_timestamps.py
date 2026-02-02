"""
Decode the timestamps seen in InfluxDB to understand the issue.
"""
from console_log import log_console

import pandas as pd

log_console("=" * 80)
log_console("DECODE: Timestamps from InfluxDB")
log_console("=" * 80)

# What the user is seeing in InfluxDB (WRONG)
wrong_ts_nano = 1767571200000000000

log_console(f"\nWRONG timestamp from InfluxDB: {wrong_ts_nano}")
wrong_ts = pd.to_datetime(wrong_ts_nano, unit='ns', utc=True)
log_console(f"  In UTC: {wrong_ts}")

wrong_ts_et = wrong_ts.tz_convert('America/New_York')
log_console(f"  In ET:  {wrong_ts_et}")
log_console(f"  Date:   {wrong_ts_et.date()} [This is what we see - WRONG]")

# What we SHOULD see (CORRECT)
correct_ts_nano = 1767589200000000000

log_console(f"\nCORRECT timestamp (expected): {correct_ts_nano}")
correct_ts = pd.to_datetime(correct_ts_nano, unit='ns', utc=True)
log_console(f"  In UTC: {correct_ts}")

correct_ts_et = correct_ts.tz_convert('America/New_York')
log_console(f"  In ET:  {correct_ts_et}")
log_console(f"  Date:   {correct_ts_et.date()} [This is what we want - CORRECT]")

# Difference
diff_nano = correct_ts_nano - wrong_ts_nano
diff_hours = diff_nano / 1e9 / 3600

log_console(f"\nDifference:")
log_console(f"  Nanoseconds: {diff_nano}")
log_console(f"  Hours:       {diff_hours}")
log_console(f"  Days:        {diff_hours / 24}")

# What generates the WRONG timestamp?
log_console("\n" + "=" * 80)
log_console("ANALYSIS: What generates the WRONG timestamp?")
log_console("=" * 80)

# If we use string "2026-01-05" directly
day_iso = "2026-01-05"

# Pandas interprets string as UTC
ts_string_as_utc = pd.to_datetime(day_iso, utc=True)
log_console(f"\nString '{day_iso}' interpreted as UTC:")
log_console(f"  Result: {ts_string_as_utc}")
log_console(f"  Nanoseconds: {int(ts_string_as_utc.value)}")
log_console(f"  Match WRONG: {int(ts_string_as_utc.value) == wrong_ts_nano}")

# If we localize to ET (CORRECT)
ts_localized_et = pd.to_datetime(day_iso).tz_localize('America/New_York')
log_console(f"\nString '{day_iso}' localized to ET:")
log_console(f"  Result: {ts_localized_et}")
log_console(f"  Nanoseconds: {int(ts_localized_et.value)}")
log_console(f"  Match CORRECT: {int(ts_localized_et.value) == correct_ts_nano}")

log_console("\n" + "=" * 80)
log_console("CONCLUSION")
log_console("=" * 80)

if int(ts_string_as_utc.value) == wrong_ts_nano:
    log_console("\n[FOUND] The WRONG timestamp matches pd.to_datetime(day_iso, utc=True)")
    log_console("This suggests the code is NOT using .tz_localize('America/New_York')")
    log_console("OR there's a conversion happening somewhere that's removing the timezone info")
else:
    log_console("\n[MYSTERY] The WRONG timestamp doesn't match expected patterns")
    log_console("Need to investigate further")

log_console("\n" + "=" * 80)
