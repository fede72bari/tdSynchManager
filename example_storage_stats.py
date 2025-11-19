#!/usr/bin/env python3
"""
Example: Get storage statistics for local databases

For real InfluxDB disk size (not estimation), configure influx_data_dir:
  - Set environment variable: TDSYNCH_INFLUX_DATA_DIR=/path/to/influxdb/data
  - Or pass as parameter: config_from_env(influx_data_dir="/path/to/data")

The data directory is where InfluxDB v3 stores Parquet files.
Find it in your InfluxDB config or startup command (--data-dir option).
"""

import sys
sys.path.insert(0, 'src')

from tdSynchManager.config import config_from_env
from tdSynchManager.manager import ThetaSyncManager

# Create manager
# For real InfluxDB disk size (not estimation), set TDSYNCH_INFLUX_DATA_DIR
# or pass influx_data_dir parameter
cfg = config_from_env()
manager = ThetaSyncManager(cfg)

print("=" * 80)
print("STORAGE STATISTICS - DETAILED VIEW")
print("=" * 80)

# Get detailed statistics
stats = manager.get_storage_stats()

def _print_view(title: str, df, columns=None):
    """Print filtered view safely even if DataFrame is empty or missing columns."""
    print(f"\n{title}")
    if df.empty:
        print("   (no data)")
        return
    if columns:
        cols = [c for c in columns if c in df.columns]
        if cols:
            print(df[cols].to_string())
            return
    print(df.to_string())

print(f"\nFound {len(stats)} data series")
print("\nDetailed statistics (sorted by size):")
_print_view("All series:", stats)

# Filter examples
print("\n" + "=" * 80)
print("FILTERED VIEWS")
print("=" * 80)

_print_view("1. Options only:", manager.get_storage_stats(asset='option'),
            ['symbol', 'interval', 'sink', 'size_mb', 'days_span'])
_print_view("2. InfluxDB only:", manager.get_storage_stats(sink='influxdb'),
            ['symbol', 'asset', 'interval', 'size_mb'])
_print_view("3. TLRY only:", manager.get_storage_stats(symbol='TLRY'),
            ['interval', 'sink', 'size_mb', 'days_span'])
_print_view("4. Tick data only:", manager.get_storage_stats(interval='tick'),
            ['symbol', 'asset', 'sink', 'size_mb'])

print("\n" + "=" * 80)
print("STORAGE SUMMARY - AGGREGATED VIEW")
print("=" * 80)

# Get aggregated summary
summary = manager.get_storage_summary()

# Total storage
print("\n📊 TOTAL STORAGE:")
total = summary['total']
print(f"   Size: {total['size_gb']:.3f} GB ({total['size_mb']:.2f} MB)")
print(f"   Series: {total['series_count']}")

# By sink
print("\n💾 BY SINK:")
for sink, data in summary['by_sink'].items():
    print(f"   {sink:12s}: {data['size_gb']:8.3f} GB  ({data['percentage']:5.1f}%)  [{data['series_count']} series]")

# By asset
print("\n📈 BY ASSET:")
for asset, data in summary['by_asset'].items():
    print(f"   {asset:12s}: {data['size_gb']:8.3f} GB  ({data['percentage']:5.1f}%)  [{data['series_count']} series]")

# By interval
print("\n⏱️  BY INTERVAL:")
for interval, data in summary['by_interval'].items():
    print(f"   {interval:12s}: {data['size_gb']:8.3f} GB  ({data['percentage']:5.1f}%)  [{data['series_count']} series]")

# Top symbols
print("\n🏆 TOP 10 SYMBOLS BY SIZE:")
for i, symbol_data in enumerate(summary['top_symbols'], 1):
    symbol = symbol_data['symbol']
    size_gb = symbol_data['size_gb']
    pct = symbol_data['percentage']
    print(f"   {i:2d}. {symbol:8s}: {size_gb:8.3f} GB  ({pct:5.1f}%)")

print("\n" + "=" * 80)
