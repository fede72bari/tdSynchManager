# InfluxDB Tests

Test scripts for InfluxDB v3 Core integration and query performance.

## Files

- `test_influx_direct.py` - Direct InfluxDB connection tests
- `test_influx_fields.py` - Field schema and data type tests
- `test_influx_health.py` - Health check and connectivity tests
- `test_influx_metadata_fast.py` - Fast metadata query tests
- `test_influx_minmax.py` - Min/max aggregation tests
- `test_influx_retry.py` - Retry logic and exponential backoff tests
- `test_influx_schema.py` - Schema validation tests
- `test_influx_tag_vs_field.py` - Tag vs field performance comparison
- `test_influx_where_tag_field.py` - WHERE clause optimization tests
- `test_influxql_first_last.py` - FIRST/LAST function tests

## Usage

These scripts test various InfluxDB v3 Core features. Ensure InfluxDB is running:

```bash
python tests/influx/test_influx_health.py
```

## Related Documentation

See [INFLUX_TIMEOUT_ANALYSIS.md](../../INFLUX_TIMEOUT_ANALYSIS.md) for timeout analysis and solutions.
