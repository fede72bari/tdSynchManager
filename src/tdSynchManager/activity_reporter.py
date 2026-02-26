from __future__ import annotations

import json
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from console_log import log_console

PHASE_DISCOVER = "discover"
PHASE_DOWNLOAD = "download"
PHASE_PERSIST = "persist"
PHASE_POSTPROCESS = "postprocess/features"

DEFAULT_SEGMENT_TIMEOUTS = {
    PHASE_DISCOVER: 900,
    PHASE_DOWNLOAD: 3600,
    PHASE_PERSIST: 1800,
    PHASE_POSTPROCESS: 1800,
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True, default=str)


def _merge_dict(base: Optional[Dict[str, Any]], extra: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(base or {})
    if not extra:
        return out
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            merged_child = dict(out[key])
            merged_child.update(value)
            out[key] = merged_child
        else:
            out[key] = value
    return out


class RedisActivityEventProducer:
    """Fail-open, non-blocking producer for Activity Contract v1 events."""

    def __init__(
        self,
        *,
        enabled: bool,
        redis_url: str = "redis://localhost:6379/0",
        stream_key: str = "gsf:activity:events:v1",
        connect_timeout_ms: int = 200,
        queue_maxsize: int = 4096,
    ) -> None:
        self.enabled = bool(enabled)
        self.stream_key = str(stream_key or "gsf:activity:events:v1")
        self.connect_timeout_s = max(_safe_int(connect_timeout_ms, 200), 1) / 1000.0
        self.queue_maxsize = max(_safe_int(queue_maxsize, 4096), 64)
        self._queue: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=self.queue_maxsize)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._redis = None
        self._dropped = 0
        self._initialized = False

        if not self.enabled:
            return

        try:
            from redis import Redis

            self._redis = Redis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=self.connect_timeout_s,
                socket_timeout=self.connect_timeout_s,
                retry_on_timeout=False,
            )
            self._thread = threading.Thread(target=self._worker_loop, name="activity-reporter", daemon=True)
            self._thread.start()
            self._initialized = True
        except Exception as exc:
            self.enabled = False
            self._redis = None
            log_console(f"[ACTIVITY][DISABLED] Redis reporter unavailable: {type(exc).__name__}: {exc}")

    def is_enabled(self) -> bool:
        return bool(self.enabled and self._initialized and self._redis is not None)

    def close(self, join_timeout_s: float = 0.2) -> None:
        if not self._thread:
            return
        self._stop_event.set()
        try:
            self._queue.put_nowait({})
        except Exception:
            pass
        self._thread.join(timeout=max(float(join_timeout_s), 0.01))

    def emit(self, payload: Dict[str, Any]) -> bool:
        if not self.is_enabled():
            return False
        try:
            self._queue.put_nowait(dict(payload))
            return True
        except queue.Full:
            self._dropped += 1
            if self._dropped % 100 == 0:
                log_console(f"[ACTIVITY][WARN] Dropped {self._dropped} events (queue full)")
            return False
        except Exception:
            return False

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                payload = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue
            if not payload:
                continue
            self._write_payload(payload)

    def _write_payload(self, payload: Dict[str, Any]) -> None:
        if self._redis is None:
            return
        fields = {
            "run_id": str(payload.get("run_id", "") or ""),
            "pipeline": str(payload.get("pipeline", "") or ""),
            "phase": str(payload.get("phase", "") or ""),
            "event_type": str(payload.get("event_type", "") or ""),
            "status": str(payload.get("status", "") or ""),
            "updated_at": str(payload.get("updated_at", "") or ""),
            "payload": _json_dumps(payload),
        }
        try:
            self._redis.xadd(self.stream_key, fields)
        except Exception:
            # Fail-open by design: never propagate.
            return


class ActivityRunReporter:
    """Stateful helper for one run_id with monotonic seq and standard metadata."""

    def __init__(
        self,
        producer: Optional[RedisActivityEventProducer],
        *,
        run_id: str,
        pipeline: str,
        phase: str = PHASE_DISCOVER,
        task_id: str = "",
        daemon_id: str = "",
        segment: str = "",
        heartbeat_interval_s: int = 30,
        segment_timeouts: Optional[Dict[str, int]] = None,
        base_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.producer = producer
        self.run_id = str(run_id or "").strip()
        self.pipeline = str(pipeline or "").strip()
        self.phase = str(phase or PHASE_DISCOVER).strip()
        self.task_id = str(task_id or "").strip()
        self.daemon_id = str(daemon_id or "").strip()
        self.segment = str(segment or "").strip()
        self.heartbeat_interval_s = max(_safe_int(heartbeat_interval_s, 30), 1)
        self._seq = -1
        self._last_hb_monotonic = 0.0
        self._base_metadata = dict(base_metadata or {})
        self._segment_timeouts = dict(DEFAULT_SEGMENT_TIMEOUTS)
        if segment_timeouts:
            for key, value in segment_timeouts.items():
                self._segment_timeouts[str(key)] = max(_safe_int(value, 1), 1)

    @classmethod
    def noop(cls) -> "ActivityRunReporter":
        return cls(
            None,
            run_id="noop",
            pipeline="noop",
            heartbeat_interval_s=30,
        )

    @property
    def enabled(self) -> bool:
        return bool(self.producer and self.producer.is_enabled() and self.run_id and self.pipeline)

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _standard_metadata(
        self,
        *,
        metadata: Optional[Dict[str, Any]] = None,
        step: str = "",
        target: Optional[Dict[str, Any]] = None,
        counters: Optional[Dict[str, Any]] = None,
        timing_ms: Optional[Dict[str, Any]] = None,
        io_context: Optional[Dict[str, Any]] = None,
        attempt: Optional[Dict[str, Any]] = None,
        watermarks: Optional[Dict[str, Any]] = None,
        block_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        out = {
            "step": str(step or ""),
            "target": dict(target or {}),
            "counters": dict(counters or {}),
            "timing_ms": dict(timing_ms or {}),
            "io_context": dict(io_context or {}),
            "attempt": dict(attempt or {}),
            "watermarks": dict(watermarks or {}),
        }
        if block_context is not None:
            out["block_context"] = dict(block_context or {})
        out = _merge_dict(self._base_metadata, out)
        out = _merge_dict(out, metadata)
        return out

    def _emit(
        self,
        *,
        event_type: str,
        status: str,
        cursor: Any = None,
        blocked_reason: str = "",
        phase: Optional[str] = None,
        segment: Optional[str] = None,
        segment_timeout_s: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        step: str = "",
        target: Optional[Dict[str, Any]] = None,
        counters: Optional[Dict[str, Any]] = None,
        timing_ms: Optional[Dict[str, Any]] = None,
        io_context: Optional[Dict[str, Any]] = None,
        attempt: Optional[Dict[str, Any]] = None,
        watermarks: Optional[Dict[str, Any]] = None,
        block_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.enabled:
            return

        phase_name = str(phase or self.phase or PHASE_DISCOVER).strip()
        segment_name = str(segment if segment is not None else self.segment).strip()
        seg_timeout = segment_timeout_s
        if seg_timeout is None and phase_name in self._segment_timeouts:
            seg_timeout = self._segment_timeouts[phase_name]

        payload: Dict[str, Any] = {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "phase": phase_name,
            "event_type": str(event_type),
            "status": str(status),
            "updated_at": utc_now_iso(),
            "seq": self._next_seq(),
            "task_id": self.task_id,
            "daemon_id": self.daemon_id,
            "segment": segment_name,
            "blocked_reason": str(blocked_reason or ""),
            "metadata": self._standard_metadata(
                metadata=metadata,
                step=step,
                target=target,
                counters=counters,
                timing_ms=timing_ms,
                io_context=io_context,
                attempt=attempt,
                watermarks=watermarks,
                block_context=block_context,
            ),
        }
        if cursor is not None:
            payload["cursor"] = cursor
        if seg_timeout is not None:
            payload["segment_timeout_s"] = max(_safe_int(seg_timeout, 1), 1)

        self.phase = phase_name
        if segment is not None:
            self.segment = segment_name
        if event_type == "heartbeat":
            self._last_hb_monotonic = time.monotonic()
        self.producer.emit(payload)

    def heartbeat_if_due(self, **kwargs: Any) -> None:
        if not self.enabled:
            return
        now = time.monotonic()
        if (now - self._last_hb_monotonic) < float(self.heartbeat_interval_s):
            return
        self.heartbeat(**kwargs)

    def run_started(self, **kwargs: Any) -> None:
        self._emit(event_type="run_started", status="running", **kwargs)

    def phase_started(self, phase: str, **kwargs: Any) -> None:
        self._emit(event_type="phase_started", status="running", phase=phase, **kwargs)

    def progress(self, **kwargs: Any) -> None:
        self._emit(event_type="progress", status="running", **kwargs)

    def heartbeat(self, **kwargs: Any) -> None:
        self._emit(event_type="heartbeat", status="running", **kwargs)

    def blocked(self, reason: str, **kwargs: Any) -> None:
        self._emit(event_type="blocked", status="blocked", blocked_reason=reason, **kwargs)

    def unblocked(self, **kwargs: Any) -> None:
        self._emit(event_type="unblocked", status="running", **kwargs)

    def phase_completed(self, **kwargs: Any) -> None:
        self._emit(event_type="phase_completed", status="running", **kwargs)

    def completed(self, **kwargs: Any) -> None:
        self._emit(event_type="completed", status="completed", **kwargs)

    def failed(self, reason: str, **kwargs: Any) -> None:
        self._emit(event_type="failed", status="failed", blocked_reason=reason, **kwargs)


def classify_block_reason(error_text: str, status_code: Optional[int] = None) -> str:
    text = str(error_text or "").lower()
    st = _safe_int(status_code, -1)
    if st == 429 or "rate limit" in text or "too many requests" in text:
        return "RATE_LIMIT"
    if "session" in text or "token" in text or st in (401, 403):
        return "SESSION_REFRESH"
    if "timeout" in text or "timed out" in text or "connect" in text:
        return "NETWORK"
    if "backpressure" in text:
        return "BACKPRESSURE"
    if "dependency" in text:
        return "DEPENDENCY_DOWN"
    if "wait" in text or "sleep" in text:
        return "WAIT_IO"
    return "UNKNOWN"


def safe_callback(callback: Optional[Callable[..., Any]], *args: Any, **kwargs: Any) -> None:
    if callback is None:
        return
    try:
        callback(*args, **kwargs)
    except Exception:
        return
