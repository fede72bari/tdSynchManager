# API Tests

Test scripts for ThetaData API endpoints.

## Files

- `test_api_dates_simple.py` - Simple date range API tests
- `test_api_dates_two_symbols.py` - Multi-symbol API tests
- `test_api_endpoints_manual.py` - Manual endpoint testing
- `test_check_es_mes.py` - ES/MES futures data tests
- `test_es_info.py` - E-mini S&P 500 info tests
- `test_nq_info.py` - E-mini NASDAQ info tests
- `test_spy_qqq_info.py` - SPY/QQQ ETF info tests
- `test_single_symbol.py` - Single symbol download tests
- `test_optimized_option_dates.py` - Optimized option date queries
- `test_option_api_blocking.py` - Option API blocking behavior tests
- `test_option_parallel_blocking.py` - Parallel option requests tests
- `test_user_tasks.py` - User-specific task tests
- `test_xom_5m_mild_skip.py` - XOM 5m data with skip logic tests

## Usage

These are standalone test scripts. Run individually:

```bash
python tests/api/test_single_symbol.py
```
