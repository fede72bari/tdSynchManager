# Changelog

All notable changes to `tdSynchManager` are documented in this file.

## [4] - 2026-02-26

### Added

- Added `src/tdSynchManager/activity_reporter.py` with a reusable Activity Contract v1 producer:
  - non-blocking Redis Stream emitter on `gsf:activity:events:v1`
  - fail-open behavior (Redis failures never stop pipeline execution)
  - monotonic per-run `seq`
  - standard metadata normalization for diagnostics (`step`, `target`, `counters`, `timing_ms`, `io_context`, `attempt`, `watermarks`, `block_context`)
  - phase defaults and segment timeout declaration for `discover`, `download`, `persist`, `postprocess/features`
- Added Activity Contract configuration fields in `src/tdSynchManager/config.py`:
  - `activity_contract_enabled`
  - `activity_pipeline_name`
  - `activity_redis_url`
  - `activity_stream_key`
  - `activity_redis_timeout_ms`
  - `activity_heartbeat_seconds`
  - `activity_queue_maxsize`
- Exported reporter API in `src/tdSynchManager/__init__.py`:
  - `ActivityRunReporter`
  - `RedisActivityEventProducer`
- Added `tests/test_activity_reporter.py` covering:
  - monotonic `seq`
  - `blocked`/`unblocked` emission
  - fail-open behavior when Redis is unavailable

### Changed

- Integrated producer lifecycle and event hooks in `src/tdSynchManager/manager.py`:
  - run lifecycle events: `run_started`, `completed`, `failed`
  - phase lifecycle events: `phase_started`, `phase_completed`
  - runtime progress/heartbeat events in significant download/persist/recovery loops
  - blocked/unblocked signals around retry and wait sections
  - activity context propagation through async run execution
- Extended retry components to emit activity callbacks:
  - `src/tdSynchManager/download_retry.py` now supports blocked/unblocked callbacks during retry waits
  - `src/tdSynchManager/influx_retry.py` now supports blocked/unblocked/progress callbacks during write retries
- Incremented runtime shell build version in `src/tdSynchManager/manager.py`:
  - `SHELL_LOG_VERSION: 3 -> 4`

### Operational Notes

- `segment_timeout_s` values are emitted as contract metadata only; timeout enforcement remains responsibility of an upper-level watchdog.
- Redis producer uses short connection/socket timeout to protect pipeline latency and preserve fail-open behavior.
