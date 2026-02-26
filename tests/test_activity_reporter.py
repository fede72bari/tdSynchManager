import threading
import time
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tdSynchManager.activity_reporter import ActivityRunReporter, RedisActivityEventProducer


class _MemoryProducer:
    def __init__(self):
        self.events = []

    def is_enabled(self):
        return True

    def emit(self, payload):
        self.events.append(dict(payload))
        return True


def test_seq_is_monotonic_for_run():
    producer = _MemoryProducer()
    reporter = ActivityRunReporter(producer, run_id="run-1", pipeline="tdsynch")

    reporter.run_started(phase="discover", step="discover_enter")
    reporter.phase_started("download", step="download_enter")
    reporter.progress(step="fetch_expirations", cursor={"chunk_id": 1})
    reporter.heartbeat(step="loop_hb")
    reporter.phase_completed(step="download_done")
    reporter.completed(step="run_done")

    seq_values = [event["seq"] for event in producer.events]
    assert seq_values == sorted(seq_values)
    assert len(set(seq_values)) == len(seq_values)
    assert producer.events[2]["metadata"]["step"] == "fetch_expirations"
    assert "target" in producer.events[2]["metadata"]
    assert "counters" in producer.events[2]["metadata"]


def test_blocked_and_unblocked_events_are_emitted():
    producer = _MemoryProducer()
    reporter = ActivityRunReporter(producer, run_id="run-2", pipeline="tdsynch")

    reporter.blocked(
        "NETWORK retry_wait",
        step="fetch_oi",
        target={"symbol": "SPY", "timeframe": "5m"},
        block_context={
            "blocked_reason_code": "NETWORK",
            "blocked_detail": "socket timeout",
            "since_ts": "2026-02-25T10:00:00+00:00",
        },
    )
    reporter.unblocked(step="fetch_oi")

    blocked_event = producer.events[0]
    unblocked_event = producer.events[1]

    assert blocked_event["event_type"] == "blocked"
    assert blocked_event["status"] == "blocked"
    assert blocked_event["blocked_reason"] == "NETWORK retry_wait"
    assert blocked_event["metadata"]["block_context"]["blocked_reason_code"] == "NETWORK"
    assert blocked_event["metadata"]["step"] == "fetch_oi"
    assert unblocked_event["event_type"] == "unblocked"
    assert unblocked_event["status"] == "running"


def test_redis_down_does_not_raise():
    class _BrokenRedis:
        def xadd(self, *args, **kwargs):
            raise OSError("redis down")

    producer = RedisActivityEventProducer(enabled=False)
    producer.enabled = True
    producer._initialized = True
    producer._redis = _BrokenRedis()
    producer._thread = threading.Thread(target=producer._worker_loop, name="activity-test-worker", daemon=True)
    producer._thread.start()

    reporter = ActivityRunReporter(producer, run_id="run-3", pipeline="tdsynch")
    reporter.run_started(phase="discover", step="discover_enter")
    reporter.progress(step="fetch_expirations", cursor={"chunk_id": 1})
    reporter.blocked(
        "NETWORK retry_wait",
        step="fetch_expirations",
        block_context={
            "blocked_reason_code": "NETWORK",
            "blocked_detail": "connection refused",
            "since_ts": "2026-02-25T10:00:00+00:00",
        },
    )
    reporter.unblocked(step="fetch_expirations")

    time.sleep(0.05)
    producer.close()
