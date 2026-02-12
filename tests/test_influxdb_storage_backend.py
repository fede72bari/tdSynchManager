from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pandas as pd

from mmdpo.storage import InfluxDBStorageBackend


class _QueryResult:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def to_pandas(self) -> pd.DataFrame:
        return self._df


class _FakeInfluxClient:
    def __init__(self, *, fail_write: bool = False) -> None:
        self.fail_write = fail_write
        self.write_calls: list[list[str]] = []
        self.query_calls: list[str] = []

    def write(self, record):
        lines = list(record)
        self.write_calls.append(lines)
        if self.fail_write:
            raise TimeoutError("write timed out")
        return True

    def query(self, query, **_kwargs):
        q = str(query)
        self.query_calls.append(q)
        q_upper = q.upper()
        if "SHOW MEASUREMENTS" in q_upper:
            return _QueryResult(pd.DataFrame({"name": []}))
        if "SELECT TRADE_DAY FROM" in q_upper:
            return _QueryResult(pd.DataFrame(columns=["trade_day"]))
        return _QueryResult(pd.DataFrame())


def _make_backend(tmp_path: Path, *, max_retries: int = 1, retry_base_delay: float = 0.0) -> InfluxDBStorageBackend:
    return InfluxDBStorageBackend(
        url="http://localhost:8181",
        bucket="unit_test_db",
        token="unit_test_token",
        write_batch=100,
        max_retries=max_retries,
        retry_base_delay=retry_base_delay,
        root_dir=tmp_path,
    )


def test_influx_write_uses_legacy_pipeline_logic(tmp_path) -> None:
    backend = _make_backend(tmp_path)
    fake_client = _FakeInfluxClient()

    backend._manager._ensure_influx_client = lambda: fake_client
    backend._manager._influx = fake_client
    backend._manager._influx_measurement_exists = lambda _measurement: False
    backend._manager._influx_available_dates_note_days = lambda _measurement, _days: None

    key = "thetadata:option:SPY:5m:influxdb"
    records = [
        {
            "timestamp": "2024-01-03T15:30:00Z",
            "symbol": "SPY",
            "expiration": "2024-01-05",
            "strike": 470,
            "right": "call",
            "close": 4.2,
        },
        {
            "timestamp": "2024-01-03T15:35:00Z",
            "symbol": "SPY",
            "expiration": "2024-01-05",
            "strike": 470,
            "right": "call",
            "close": 4.3,
        },
    ]

    wrote = asyncio.run(backend.write_records(dataset_key=key, records=records))

    assert wrote == 2
    assert fake_client.write_calls
    assert any("SPY-option-5m" in line for line in fake_client.write_calls[0])


def test_influx_failed_batches_are_saved_for_recovery(tmp_path) -> None:
    backend = _make_backend(tmp_path, max_retries=1, retry_base_delay=0.0)
    fake_client = _FakeInfluxClient(fail_write=True)

    backend._manager._ensure_influx_client = lambda: fake_client
    backend._manager._influx = fake_client
    backend._manager._influx_measurement_exists = lambda _measurement: False
    backend._manager._influx_available_dates_note_days = lambda _measurement, _days: None

    key = "thetadata:stock:AAPL:1d:influxdb"
    records = [
        {"timestamp": "2024-01-03T21:00:00Z", "symbol": "AAPL", "close": 181.0},
    ]

    wrote = asyncio.run(backend.write_records(dataset_key=key, records=records))
    pending, recovered = backend.list_failed_batches()

    assert wrote == 0
    assert recovered == []
    assert len(pending) >= 1

    with open(pending[0], "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert "measurement" in payload
    assert payload.get("num_lines", 0) >= 1


def test_influx_failed_batch_recovery_flow(tmp_path) -> None:
    backend = _make_backend(tmp_path, max_retries=1, retry_base_delay=0.0)
    failing_client = _FakeInfluxClient(fail_write=True)

    backend._manager._ensure_influx_client = lambda: failing_client
    backend._manager._influx = failing_client
    backend._manager._influx_measurement_exists = lambda _measurement: False
    backend._manager._influx_available_dates_note_days = lambda _measurement, _days: None

    key = "thetadata:stock:AAPL:1d:influxdb"
    records = [{"timestamp": "2024-01-03T21:00:00Z", "symbol": "AAPL", "close": 181.0}]
    asyncio.run(backend.write_records(dataset_key=key, records=records))

    success_client = _FakeInfluxClient(fail_write=False)
    backend._manager._ensure_influx_client = lambda: success_client
    backend._manager._influx = success_client

    backend.recover_failed_batches(dry_run=False, delete_recovered=True)
    pending, _recovered = backend.list_failed_batches()

    assert pending == []
    assert success_client.write_calls


def test_influx_read_and_list_series_delegate_to_legacy_inventory(tmp_path) -> None:
    backend = _make_backend(tmp_path)

    backend._manager.query_local_data = lambda **_kwargs: (
        pd.DataFrame(
            [
                {"timestamp_utc": "2024-01-03T21:00:00Z", "symbol": "AAPL", "close": 181.0},
                {"timestamp_utc": "2024-01-04T21:00:00Z", "symbol": "AAPL", "close": 182.0},
            ]
        ),
        [],
    )
    backend._manager.list_available_data = lambda **_kwargs: pd.DataFrame(
        [
            {"asset": "option", "symbol": "SPY", "interval": "5m"},
            {"asset": "stock", "symbol": "AAPL", "interval": "1d"},
        ]
    )

    rows = asyncio.run(
        backend.read_records(
            dataset_key="thetadata:stock:AAPL:1d:influxdb",
            start_date="2024-01-03",
            end_date="2024-01-04",
            limit=1,
        )
    )
    series = asyncio.run(backend.list_series())

    assert len(rows) == 1
    assert series == [
        "thetadata:option:SPY:5m:influxdb",
        "thetadata:stock:AAPL:1d:influxdb",
    ]
