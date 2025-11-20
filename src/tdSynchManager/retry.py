"""Retry utilities with configurable policy and automatic logging."""

import asyncio
from typing import Any, Awaitable, Callable, Dict, Tuple, Type, TypeVar

from .config import RetryPolicy
from .logger import DataConsistencyLogger

__all__ = ["retry_with_policy"]

T = TypeVar('T')


async def retry_with_policy(
    coro_func: Callable[[], Awaitable[T]],
    policy: RetryPolicy,
    logger: DataConsistencyLogger,
    context: Dict[str, Any],
    error_types: Tuple[Type[Exception], ...] = (Exception,)
) -> Tuple[T | None, bool]:
    """Execute coroutine with retry policy and automatic logging.

    Parameters
    ----------
    coro_func : Callable[[], Awaitable[T]]
        Async function to execute (should be a lambda or callable that returns a coroutine).
    policy : RetryPolicy
        Retry policy configuration.
    logger : DataConsistencyLogger
        Logger instance for recording retry attempts.
    context : Dict[str, Any]
        Context dictionary with keys: symbol, asset, interval, date_range (tuple).
    error_types : Tuple[Type[Exception], ...], optional
        Tuple of exception types to catch and retry (default: Exception).

    Returns
    -------
    Tuple[T | None, bool]
        Tuple of (result, success). Returns (result, True) on success,
        (None, False) on failure after all retries exhausted.

    Examples
    --------
    >>> result, success = await retry_with_policy(
    ...     coro_func=lambda: client.stock_history_eod("AAPL", "2024-01-01", "2024-01-31", "csv"),
    ...     policy=config.retry_policy,
    ...     logger=logger,
    ...     context={
    ...         'symbol': 'AAPL',
    ...         'asset': 'stock',
    ...         'interval': '1d',
    ...         'date_range': ('2024-01-01', '2024-01-31')
    ...     }
    ... )
    >>> if success:
    ...     # Process result
    ...     pass
    """
    last_error: Exception | None = None
    symbol = context.get('symbol', 'UNKNOWN')
    asset = context.get('asset', 'UNKNOWN')
    interval = context.get('interval', 'UNKNOWN')
    date_range = context.get('date_range', ('', ''))

    for attempt in range(policy.max_attempts):
        try:
            result = await coro_func()

            # Success
            if attempt > 0:
                # Log resolution if this was a retry
                logger.log_resolution(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=date_range,
                    message=f"Resolved after {attempt + 1} attempts",
                    details={'total_attempts': attempt + 1}
                )

            return (result, True)

        except error_types as e:
            last_error = e

            if attempt < policy.max_attempts - 1:
                # Log retry attempt
                logger.log_retry_attempt(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=date_range,
                    attempt=attempt + 1,
                    error_msg=str(e),
                    details={'error_type': type(e).__name__}
                )

                # Wait before retry
                await asyncio.sleep(policy.delay_seconds)
            else:
                # Final failure after all retries
                logger.log_failure(
                    symbol=symbol,
                    asset=asset,
                    interval=interval,
                    date_range=date_range,
                    message=f"Failed after {policy.max_attempts} attempts: {e}",
                    details={
                        'error_type': type(e).__name__,
                        'total_attempts': policy.max_attempts
                    }
                )

        except asyncio.CancelledError:
            # Always re-raise CancelledError
            raise

    # All retries exhausted
    return (None, False)
