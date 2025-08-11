"""
Error handling utilities for the tradingAPI package.

This module provides utilities for robust error handling, retry logic,
and error recovery mechanisms.
"""

import functools
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Type, Union

from .exceptions import TradingAPIError, RetryableError, NonRetryableError, is_retryable_error, create_error_context
from . import trading_logger

# Global flag to disable execution time logging for debugging
DISABLE_EXECUTION_TIME_LOGGING = False

# Global flag to disable retry functionality for debugging
DISABLE_RETRY = False


def set_execution_time_logging(enabled: bool = True):
    """Enable or disable execution time logging globally.

    Args:
        enabled: If True, enable execution time logging. If False, disable it.
    """
    global DISABLE_EXECUTION_TIME_LOGGING
    DISABLE_EXECUTION_TIME_LOGGING = not enabled


def set_retry_enabled(enabled: bool = True):
    """Enable or disable retry functionality globally.

    Args:
        enabled: If True, enable retry functionality. If False, disable it.
    """
    global DISABLE_RETRY
    DISABLE_RETRY = not enabled


def retry_on_error(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
    retry_condition: Optional[Callable[[Exception], bool]] = None,
    on_retry: Optional[Callable[[int, Exception], None]] = None,
    on_final_failure: Optional[Callable[[Exception], None]] = None,
):
    """
    Decorator to retry functions on specific exceptions.

    Args:
        max_retries: Maximum number of retry attempts.
        delay: Initial delay between retries in seconds.
        backoff_factor: Multiplier for delay on each retry.
        exceptions: Tuple of exceptions to catch and retry.
        retry_condition: Optional function to determine if error is retryable.
        on_retry: Optional callback called before each retry.
        on_final_failure: Optional callback called on final failure.
    """

    def decorator(func: Callable) -> Callable:
        # Skip retry logic if disabled globally
        if DISABLE_RETRY:
            return func

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    # Check if we should retry
                    should_retry = (
                        attempt < max_retries
                        and (retry_condition is None or retry_condition(e))
                        and (is_retryable_error(e) if hasattr(e, "__class__") else True)
                    )

                    if not should_retry:
                        break

                    # Log retry attempt
                    trading_logger.log_warning(
                        f"Retry attempt {attempt + 1}/{max_retries + 1} for {func.__name__}",
                        {
                            "function": func.__name__,
                            "attempt": attempt + 1,
                            "max_retries": max_retries,
                            "error_type": type(e).__name__,
                            "error_message": str(e),
                            "delay": current_delay,
                        },
                    )

                    # Call on_retry callback
                    if on_retry:
                        on_retry(attempt + 1, e)

                    # Wait before retry
                    if attempt < max_retries:
                        time.sleep(current_delay)
                        current_delay *= backoff_factor

            # Call on_final_failure callback
            if on_final_failure and last_exception:
                on_final_failure(last_exception)

            raise last_exception

        return wrapper

    return decorator


def safe_execute(
    func: Callable,
    *args,
    default_return: Any = None,
    log_errors: bool = True,
    context: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Any:
    """
    Safely execute a function with error handling.

    Args:
        func: Function to execute.
        *args: Positional arguments for the function.
        default_return: Value to return on error.
        log_errors: Whether to log errors.
        context: Additional context for error logging.
        **kwargs: Keyword arguments for the function.

    Returns:
        Function result or default_return on error.
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if log_errors:
            error_context = context or {}
            error_context.update({"function": func.__name__, "args": str(args), "kwargs": str(kwargs)})
            trading_logger.log_error(f"Error executing {func.__name__}", e, error_context)
        return default_return


def validate_inputs(**validations: Dict[str, Any]) -> Callable:
    """
    Decorator to validate function inputs.

    Args:
        **validations: Dictionary mapping parameter names to validation functions.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get function signature
            import inspect

            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            # Validate each parameter
            for param_name, validation_func in validations.items():
                if param_name in bound_args.arguments:
                    value = bound_args.arguments[param_name]
                    try:
                        if not validation_func(value):
                            raise ValueError(f"Validation failed for parameter '{param_name}'")
                    except Exception as e:
                        trading_logger.log_error(
                            f"Input validation failed for {func.__name__}",
                            e,
                            {
                                "function": func.__name__,
                                "parameter": param_name,
                                "value": str(value),
                                "validation_func": validation_func.__name__,
                            },
                        )
                        raise

            return func(*args, **kwargs)

        return wrapper

    return decorator


def log_execution_time(func: Callable) -> Callable:
    """Decorator to log function execution time.

    Can be disabled globally using set_execution_time_logging(False) for debugging.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Skip logging if disabled globally
        if DISABLE_EXECUTION_TIME_LOGGING:
            return func(*args, **kwargs)

        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            trading_logger.log_debug(
                f"Function {func.__name__} completed successfully",
                {"function": func.__name__, "execution_time": execution_time, "success": True},
            )
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            trading_logger.log_error(
                f"Function {func.__name__} failed",
                e,
                {"function": func.__name__, "execution_time": execution_time, "success": False},
            )
            raise

    return wrapper


def handle_broker_errors(func: Callable) -> Callable:
    """Decorator to handle broker-specific errors."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Add broker context to error
            context = create_error_context(function=func.__name__, args=str(args), kwargs=str(kwargs))

            # Categorize broker errors
            if "connection" in str(e).lower() or "timeout" in str(e).lower():
                from .exceptions import BrokerConnectionError

                raise BrokerConnectionError(f"Broker connection error: {str(e)}", context)
            elif "authentication" in str(e).lower() or "login" in str(e).lower():
                from .exceptions import AuthenticationError

                raise AuthenticationError(f"Authentication error: {str(e)}", context)
            elif "rate limit" in str(e).lower() or "throttle" in str(e).lower():
                from .exceptions import RateLimitError

                raise RateLimitError(f"Rate limit exceeded: {str(e)}", context)
            else:
                # Re-raise with context
                if isinstance(e, TradingAPIError):
                    e.context.update(context)
                else:
                    from .exceptions import BrokerConnectionError

                    raise BrokerConnectionError(f"Broker error: {str(e)}", context)

    return wrapper


def create_error_recovery_handler(
    error_types: List[Type[Exception]], recovery_func: Callable, max_recovery_attempts: int = 1
) -> Callable:
    """
    Create an error recovery handler.

    Args:
        error_types: List of exception types to handle.
        recovery_func: Function to call for recovery.
        max_recovery_attempts: Maximum recovery attempts.

    Returns:
        Decorator function.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_recovery_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except tuple(error_types) as e:
                    if attempt < max_recovery_attempts:
                        trading_logger.log_warning(
                            f"Attempting recovery for {func.__name__}",
                            {
                                "function": func.__name__,
                                "error_type": type(e).__name__,
                                "recovery_attempt": attempt + 1,
                                "max_recovery_attempts": max_recovery_attempts,
                            },
                        )
                        try:
                            recovery_func(e, *args, **kwargs)
                        except Exception as recovery_error:
                            trading_logger.log_error(
                                f"Recovery failed for {func.__name__}",
                                recovery_error,
                                {"function": func.__name__, "original_error": str(e), "recovery_attempt": attempt + 1},
                            )
                    else:
                        raise

        return wrapper

    return decorator


def log_function_entry_exit(func: Callable) -> Callable:
    """Decorator to log function entry and exit."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        trading_logger.log_debug(
            f"Entering {func.__name__}",
            {"function": func.__name__, "args_count": len(args), "kwargs_count": len(kwargs)},
        )

        try:
            result = func(*args, **kwargs)
            trading_logger.log_debug(
                f"Exiting {func.__name__} successfully",
                {"function": func.__name__, "result_type": type(result).__name__},
            )
            return result
        except Exception as e:
            trading_logger.log_error(f"Exiting {func.__name__} with error", e, {"function": func.__name__})
            raise

    return wrapper
