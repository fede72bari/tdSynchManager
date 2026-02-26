# MMDPO Integration Report - Activity Contract v1 Producer

Date: 2026-02-26

## 1) Goal

Integrate the new Activity Contract v1 producer in MMDPO so pipeline progress is emitted on Redis stream:

- `gsf:activity:events:v1`

without coupling activity emission to restart logic.

## 2) Producer Library Already Available

Reusable producer components are already in tdSynchManager:

- `src/tdSynchManager/activity_reporter.py`
  - `RedisActivityEventProducer` (fail-open, non-blocking queue + background worker)
  - `ActivityRunReporter` (run state, monotonic `seq`, event helpers)

Config toggles already available:

- `src/tdSynchManager/config.py`
  - `activity_contract_enabled`
  - `activity_pipeline_name`
  - `activity_redis_url`
  - `activity_stream_key`
  - `activity_redis_timeout_ms`
  - `activity_heartbeat_seconds`
  - `activity_queue_maxsize`

## 3) Event Types Emitted

The producer supports all required contract events:

- `run_started`
- `phase_started`
- `progress`
- `heartbeat`
- `blocked`
- `unblocked`
- `phase_completed`
- `completed`
- `failed`

Standard phases used in code:

- `discover` (900s declared as `segment_timeout_s`)
- `download` (3600s)
- `persist` (1800s)
- `postprocess/features` (1800s)

Note: `segment_timeout_s` is declarative metadata for watchdog observability. It is not enforced by producer logic.

## 4) Where Messages Are Emitted (Current tdSynchManager)

### Run lifecycle

- `manager._spawn_sync(...)`
  - emits `run_started` at run begin
  - emits `completed` on success
  - emits `failed` on exception/cancel

### Discover phase

- `manager._sync_symbol(...)`
  - `phase_started` (`discover_resume_window`)
  - `progress` (`discover_bounds`)
  - `phase_completed` (`discover_completed`)

### Download phase

- `manager._sync_symbol(...)` main loop
  - `phase_started` (`download_loop_start`)
  - `heartbeat` in long loops (`download_retro_loop`, `download_mild_skip_loop`, `download_eod_batch_loop`, `download_day_loop`)
  - `progress` for fetch boundaries (`fetch_ohlc`, `fetch_ohlc_done`)
  - `phase_completed` (`download_completed`)

- `manager._download_and_store_options(...)`
  - `phase_started` + `progress` (`download_day_start`)
  - `progress` (`fetch_expirations`, `fetch_expirations_done`)
  - per-expiration OI loop: `heartbeat`/`progress` (`fetch_oi`, `fetch_oi_done`)
  - persist phase events (EOD + intraday branches)

- `manager._download_and_store_equity_or_index(...)`
  - `phase_started` + `progress` (`download_day_start`)
  - persist phase events (`persist_start`, `persist_completed`)

### Provider/retry blocked-unblocked

- `manager._td_get_with_retry(...)`
  - `heartbeat` + `progress` on request attempt (`provider_request`)
  - `progress` on success (`provider_request_done`)
  - `blocked` before retry sleep (`provider_retry_wait`)
  - `unblocked` after retry sleep (`provider_retry_resume`)

- `download_retry.download_with_retry_and_validation(...)`
  - emits `blocked`/`unblocked` via callback hooks during retry waits
  - steps: `fetch_retry_wait`, `fetch_retry_resume`

- `influx_retry.InfluxWriteRetry.write_with_retry(...)`
  - emits `blocked`/`unblocked` + `progress` via callbacks
  - steps: `write_influx_retry_wait`, `write_influx_retry_resume`, `write_influx_attempt`

### Persist phase and storage loops

- `manager._append_influx_df(...)`
  - `heartbeat` in batch loop (`write_influx_batch_loop`)
  - `progress` before and after each batch (`write_influx`, `write_influx_done`)

- `manager._append_csv_text(...)`
  - `progress` (`write_csv`)

- `manager._append_parquet_df(...)`
  - `progress` (`write_parquet`)

### Postprocess/features/coherence

- `manager._record_missing_enrichment_rows(...)`
  - `phase_started` (`compute_features_missing_rows`)
  - `progress` (`compute_features`)
  - `phase_completed` (`compute_features_completed`)

- `manager.check_and_recover_coherence(...)`
  - `phase_started` (`coherence_check_start`)
  - `progress` (`coherence_check_start`, `coherence_check_done`, `coherence_check_post_recovery`)
  - `phase_completed` (`coherence_phase_completed`)

## 5) Function Signatures Already Changed (Shared Components)

### New module API

- `RedisActivityEventProducer.__init__(*, enabled: bool, redis_url: str = "redis://localhost:6379/0", stream_key: str = "gsf:activity:events:v1", connect_timeout_ms: int = 200, queue_maxsize: int = 4096) -> None`
- `ActivityRunReporter.__init__(producer, *, run_id: str, pipeline: str, phase: str = "discover", task_id: str = "", daemon_id: str = "", segment: str = "", heartbeat_interval_s: int = 30, segment_timeouts: dict | None = None, base_metadata: dict | None = None) -> None`

### Retry hooks

- `download_with_retry_and_validation(download_func, parse_func, validate_func, retry_policy, logger, context: dict) -> Tuple[pd.DataFrame, bool]`
  - `context` now supports hook keys:
    - `on_blocked`
    - `on_unblocked`

- `InfluxWriteRetry.write_with_retry(self, client, lines, measurement, batch_idx, metadata=None, on_blocked=None, on_unblocked=None, on_progress=None) -> bool`

## 6) Function Signatures To Change In MMDPO

Current MMDPO entrypoint is:

- `src/mmdpo/storage/influxdb_storage_backend.py`

To integrate Activity Contract end-to-end for MMDPO, change these signatures.

### A) Backend constructor

Current:

- `InfluxDBStorageBackend.__init__(..., root_dir: str | Path = ".mmdpo_cache", provider_hint: str = "thetadata")`

Proposed:

- `InfluxDBStorageBackend.__init__(..., root_dir: str | Path = ".mmdpo_cache", provider_hint: str = "thetadata", activity_contract_enabled: bool = False, activity_pipeline_name: str = "mmdpo", activity_redis_url: str = "redis://localhost:6379/0", activity_stream_key: str = "gsf:activity:events:v1", activity_redis_timeout_ms: int = 200, activity_heartbeat_seconds: int = 30, activity_queue_maxsize: int = 4096)`

Reason: pass-through these values into `ManagerConfig` so producer can be enabled per environment.

### B) Write API with run metadata

Current:

- `write_records(self, *, dataset_key: str, records: Sequence[Mapping[str, Any]], mode: str = "append") -> int`

Proposed:

- `write_records(self, *, dataset_key: str, records: Sequence[Mapping[str, Any]], mode: str = "append", activity_task_id: str | None = None, activity_daemon_id: str | None = None, activity_metadata: Mapping[str, Any] | None = None) -> int`

Reason: allow caller/session to pass run identifiers and diagnostics for GSF tracking.

### C) Optional proxy extension for direct writer

Current:

- `_append_influx_df(self, base_path: str, df_new) -> int`

Proposed:

- `_append_influx_df(self, base_path: str, df_new, *, activity_target: Mapping[str, Any] | None = None) -> int`

Reason: preserve hooks when writing directly from backend adapters.

## 7) Minimal MMDPO Integration Flow

Inside `write_records(...)`, before calling `_manager._append_influx_df(...)`:

1. Build activity run (`run_id`, `pipeline="mmdpo"`, `target` from dataset key).
2. Emit `run_started`.
3. Emit `phase_started` for `persist`.
4. Call `_append_influx_df(...)` (this already emits batch progress and retry blocked/unblocked via existing hooks).
5. Emit `phase_completed`.
6. Emit `completed` or `failed`.

Important:

- Keep producer fail-open (never raise if Redis is down).
- Keep Redis timeout short (`<=200ms`).
- Always include `step`, `target`, `counters`, `attempt`, `timing_ms`, `io_context`, `watermarks` in `metadata` where available.

