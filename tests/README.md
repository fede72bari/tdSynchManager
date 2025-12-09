# Tests Directory

This directory contains all test scripts organized by category.

## Structure

```
tests/
├── README.md                     # This file
├── test_strict_mode.py          # Strict mode implementation tests
├── test_trace_error.py          # Error tracing tests
├── test_column_verification.py  # Column schema verification tests
├── test_tick_fresh_table.py     # Fresh table tick data tests
├── api/                         # ThetaData API tests
│   ├── README.md
│   └── test_*.py
├── influx/                      # InfluxDB v3 Core tests
│   ├── README.md
│   └── test_*.py
└── analysis/                    # Analysis and utility scripts
    ├── README.md
    └── *.py
```

## Running Tests

Individual test scripts can be run directly:

```bash
# Test strict mode implementation
python tests/test_strict_mode.py

# Test API endpoints
python tests/api/test_single_symbol.py

# Test InfluxDB connectivity
python tests/influx/test_influx_health.py

# Run analysis scripts
python tests/analysis/analyze_worst_cases.py
```

## Test Categories

- **Root tests**: Core functionality and critical feature tests
- **api/**: ThetaData API endpoint and data retrieval tests
- **influx/**: InfluxDB v3 Core integration and performance tests
- **analysis/**: Data analysis, performance profiling, and utilities

## Related Documentation

- [STRICT_MODE_CHANGES.md](../STRICT_MODE_CHANGES.md) - Strict mode documentation
- [INFLUX_TIMEOUT_ANALYSIS.md](../INFLUX_TIMEOUT_ANALYSIS.md) - InfluxDB timeout analysis
- [OPTIMIZATION_SUMMARY.md](../OPTIMIZATION_SUMMARY.md) - Performance optimization results
