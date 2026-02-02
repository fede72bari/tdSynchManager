"""Console logging utilities with structured formatting and verbosity control."""

from __future__ import annotations

import builtins
import inspect
import re
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

__all__ = [
    "log_console",
    "set_log_verbosity",
    "get_log_verbosity",
    "set_log_context",
    "reset_log_context",
]

_ALLOWED_LOG_TYPES = {"OPERATION", "WARNING", "ERROR", "DEBUG"}
_DEFAULT_LOG_VERBOSITY = 3
_LOG_VERBOSITY = _DEFAULT_LOG_VERBOSITY
_LOG_CONTEXT: ContextVar[Optional[dict]] = ContextVar("tdsynch_log_context", default=None)
_DEBUG_TAGS = {
    "DEBUG",
    "TRACE",
    "DIAG",
    "SAMPLE",
    "DUMP",
    "INFLUX-TS-CONV",
    "TS-CONV",
    "TS_CONV",
    "DEBUG-EDOI",
    "DEBUG-MISSING",
    "DEBUG-OVERLAP",
    "DEBUG-TS",
}
_DEBUG_KEYWORDS = {
    "DEBUG",
    "TRACE",
    "DIAG",
    "SAMPLE",
    "DUMP",
    "TS-CONV",
    "TS_CONV",
    "TIMESTAMP CONV",
    "CONVERTED TO NANOSECONDS",
}


def set_log_verbosity(level: int) -> None:
    """Set the global console log verbosity (0-3)."""
    global _LOG_VERBOSITY
    try:
        _LOG_VERBOSITY = int(level)
    except Exception:
        _LOG_VERBOSITY = _DEFAULT_LOG_VERBOSITY


def get_log_verbosity() -> int:
    """Return the current global console log verbosity."""
    return _LOG_VERBOSITY


def set_log_context(
    *,
    symbol: Any = None,
    asset: Any = None,
    interval: Any = None,
    class_func: Optional[str] = None,
):
    """Set a context for subsequent log lines (useful for async tasks)."""
    return _LOG_CONTEXT.set(
        {
            "symbol": symbol,
            "asset": asset,
            "interval": interval,
            "class_func": class_func,
        }
    )


def reset_log_context(token) -> None:
    """Reset context to the previous value."""
    _LOG_CONTEXT.reset(token)


def _get_raw_print():
    current_print = builtins.print
    original_print = getattr(current_print, "_original_print", None)
    return original_print if callable(original_print) else current_print


def _format_symbol_type_tf(symbol: Any, asset: Any, interval: Any) -> str:
    def _norm(value: Any, fallback: str) -> str:
        if value is None:
            return fallback
        text = str(value).strip()
        return text if text and text != "None" else fallback

    if symbol is None and asset is None and interval is None:
        return "SYSTEM-NA-NA"

    symbol_val = _norm(symbol, "NA")
    asset_val = _norm(asset, "NA")
    interval_val = _norm(interval, "NA")
    return f"{symbol_val}-{asset_val}-{interval_val}"


def _infer_context(frame) -> Tuple[Optional[Any], Optional[Any], Optional[Any], Optional[str]]:
    if frame is None:
        return None, None, None, None

    symbol = None
    asset = None
    interval = None
    class_func = None

    f_locals = frame.f_locals or {}
    if "symbol" in f_locals:
        symbol = f_locals.get("symbol")
    if "asset" in f_locals:
        asset = f_locals.get("asset")
    if "interval" in f_locals:
        interval = f_locals.get("interval")
    elif "tf" in f_locals:
        interval = f_locals.get("tf")
    elif "timeframe" in f_locals:
        interval = f_locals.get("timeframe")

    func_name = frame.f_code.co_name if frame else None
    if func_name == "<module>":
        func_name = frame.f_globals.get("__name__", None)

    if "self" in f_locals:
        try:
            class_name = f_locals["self"].__class__.__name__
        except Exception:
            class_name = None
    elif "cls" in f_locals and isinstance(f_locals.get("cls"), type):
        class_name = f_locals["cls"].__name__
    else:
        class_name = None

    if class_name and func_name:
        class_func = f"{class_name}.{func_name}"
    elif func_name:
        class_func = func_name

    return symbol, asset, interval, class_func


def _detect_log_type(message: str) -> str:
    upper = message.upper()
    if (
        "ERROR" in upper
        or "FATAL" in upper
        or "CRITICAL" in upper
        or "ALERT" in upper
        or "FAIL" in upper
        or "EXCEPTION" in upper
        or "TRACEBACK" in upper
    ):
        return "ERROR"
    if "WARN" in upper:
        return "WARNING"
    tags = re.findall(r"\[([A-Za-z0-9_-]+)\]", message)
    for tag in tags:
        up_tag = tag.upper()
        if "DEBUG" in up_tag or up_tag in _DEBUG_TAGS or up_tag.endswith("TRACE"):
            return "DEBUG"
    for keyword in _DEBUG_KEYWORDS:
        if keyword in upper:
            return "DEBUG"
    if "DEBUG" in upper:
        return "DEBUG"
    return "OPERATION"


def _normalize_log_type(log_type: Optional[str], message: str) -> str:
    if log_type:
        normalized = str(log_type).upper()
        if normalized in _ALLOWED_LOG_TYPES:
            return normalized
    detected = _detect_log_type(message)
    return detected if detected in _ALLOWED_LOG_TYPES else "DEBUG"


def _min_verbosity_for(log_type: str) -> int:
    if log_type in ("ERROR", "WARNING"):
        return 1
    if log_type == "OPERATION":
        return 2
    return 3


def log_console(
    *args: Any,
    log_type: Optional[str] = None,
    symbol: Any = None,
    asset: Any = None,
    interval: Any = None,
    class_func: Optional[str] = None,
    log_verbosity: Optional[int] = None,
    sep: str = " ",
    end: str = "\n",
) -> None:
    """Structured console logger.

    Output format:
        [time_stamp][simbolo_tipo_TF][TIPO_LOG][classe_funzione][messaggio]
    """
    if not args:
        return

    message = sep.join(str(arg) for arg in args)
    if end != "\n":
        message = f"{message}{end}"

    ctx = _LOG_CONTEXT.get()
    if ctx:
        if symbol is None:
            symbol = ctx.get("symbol")
        if asset is None:
            asset = ctx.get("asset")
        if interval is None:
            interval = ctx.get("interval")
        if class_func is None:
            class_func = ctx.get("class_func")

    frame = inspect.currentframe()
    caller = frame.f_back if frame else None
    try:
        if symbol is None or asset is None or interval is None or class_func is None:
            inf_symbol, inf_asset, inf_interval, inf_class_func = _infer_context(caller)
            if symbol is None:
                symbol = inf_symbol
            if asset is None:
                asset = inf_asset
            if interval is None:
                interval = inf_interval
            if class_func is None:
                class_func = inf_class_func
    finally:
        del frame

    if not class_func:
        class_func = "UNKNOWN"

    symbol_type_tf = _format_symbol_type_tf(symbol, asset, interval)
    log_type_final = _normalize_log_type(log_type, message)

    verbosity = _LOG_VERBOSITY if log_verbosity is None else int(log_verbosity)
    if verbosity <= 0:
        return
    if verbosity < _min_verbosity_for(log_type_final):
        return

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    prefix = f"[{ts}][{symbol_type_tf}][{log_type_final}][{class_func}]"
    raw_print = _get_raw_print()

    lines = message.splitlines() or [""]
    for line in lines:
        raw_print(f"{prefix}[{line}]")
