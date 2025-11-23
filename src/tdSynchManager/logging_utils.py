"""Utilities for consistent, timestamped logging."""

from __future__ import annotations

import builtins
from datetime import datetime, timezone
from typing import Any

_PATCH_FLAG = "_tdsynch_timestamped_print"


def install_timestamped_print() -> None:
    """Global override for built-in print that prefixes each line with an ISO timestamp."""
    current_print = builtins.print
    if getattr(current_print, _PATCH_FLAG, False):
        return

    original_print = current_print

    def timestamped_print(*args: Any, **kwargs: Any) -> None:
        if not args:
            original_print(*args, **kwargs)
            return

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        new_args = (f"[{ts}] {args[0]}", *args[1:])
        original_print(*new_args, **kwargs)

    setattr(timestamped_print, _PATCH_FLAG, True)
    setattr(timestamped_print, "_original_print", original_print)
    builtins.print = timestamped_print
