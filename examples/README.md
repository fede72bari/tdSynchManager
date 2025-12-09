# Examples Directory

Example scripts and Jupyter notebooks demonstrating ThetaSyncManager usage.

## Files

### Jupyter Notebooks

- **ThetaDataManager_Examples.ipynb** - Comprehensive usage examples covering:
  - Basic configuration and setup
  - Stock and index data downloads
  - Option chain downloads with Greeks/IV/OI
  - Coherence checking and recovery
  - Advanced features (custom tasks, retry logic, etc.)

### Python Scripts

- **example_notebook_autoclear.py** - Example of Jupyter notebook output auto-clearing
  - Demonstrates OutputManager usage
  - Automatic cell output cleanup
  - IPython integration

## Usage

### Running Jupyter Notebooks

```bash
jupyter notebook examples/ThetaDataManager_Examples.ipynb
```

See [NOTEBOOK_USAGE.md](../NOTEBOOK_USAGE.md) for best practices.

### Running Python Examples

```bash
python examples/example_notebook_autoclear.py
```

## Related Documentation

- [MANUAL.md](../MANUAL.md) - Complete user and developer manual
- [NOTEBOOK_USAGE.md](../NOTEBOOK_USAGE.md) - Jupyter notebook guidelines
- [STRICT_MODE_CHANGES.md](../STRICT_MODE_CHANGES.md) - Strict mode features

## Example Workflows

### Basic Stock Download

```python
from src.tdSynchManager import ThetaSyncManager

async def download_stock():
    mgr = ThetaSyncManager('config.yaml')
    await mgr.run_stock_intraday(
        symbol='SPY',
        interval='5m',
        start_date='2024-01-01',
        end_date='2024-01-31',
        sink='influxdb'
    )
```

### Option Chain with Greeks

```python
async def download_options():
    mgr = ThetaSyncManager('config.yaml')
    await mgr.run_option_intraday(
        symbol='SPY',
        interval='5m',
        start_date='2024-01-01',
        end_date='2024-01-31',
        enrich_greeks=True,  # Include Greeks/IV/OI
        sink='influxdb'
    )
```

### Coherence Check and Recovery

```python
from src.tdSynchManager.coherence import CoherenceChecker, IncoherenceRecovery

async def check_and_recover():
    mgr = ThetaSyncManager('config.yaml')
    checker = CoherenceChecker(mgr)

    # Check data completeness
    report = await checker.check(
        symbol='SPY',
        asset='option',
        interval='5m',
        start_date='2024-01-01',
        end_date='2024-01-31'
    )

    # Recover if issues found
    if not report.is_coherent:
        recovery = IncoherenceRecovery(mgr)
        result = await recovery.recover(report, fix=True)
```

See the Jupyter notebook for complete working examples with output.
