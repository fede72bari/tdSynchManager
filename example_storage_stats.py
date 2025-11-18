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

print(f"\nFound {len(stats)} data series")
print("\nDetailed statistics (sorted by size):")
print(stats.to_string())

# Filter examples
print("\n" + "=" * 80)
print("FILTERED VIEWS")
print("=" * 80)

# Only options
print("\n1. Options only:")
options_stats = manager.get_storage_stats(asset='option')
print(options_stats[['symbol', 'interval', 'sink', 'size_mb', 'days_span']].to_string())

# Only InfluxDB
print("\n2. InfluxDB only:")
influx_stats = manager.get_storage_stats(sink='influxdb')
print(influx_stats[['symbol', 'asset', 'interval', 'size_mb']].to_string())

# Only TLRY
print("\n3. TLRY only:")
tlry_stats = manager.get_storage_stats(symbol='TLRY')
print(tlry_stats[['interval', 'sink', 'size_mb', 'days_span']].to_string())

# Only tick data
print("\n4. Tick data only:")
tick_stats = manager.get_storage_stats(interval='tick')
print(tick_stats[['symbol', 'asset', 'sink', 'size_mb']].to_string())

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
