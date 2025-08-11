"""
Custom exceptions for the tradingAPI package.

This module defines custom exception classes that provide better error categorization
and context for debugging and error handling throughout the tradingAPI package.
"""

from typing import Any, Dict, Optional


class TradingAPIError(Exception):
    """Base exception class for all tradingAPI errors."""

    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.context = context or {}

    def __str__(self):
        context_str = f" (Context: {self.context})" if self.context else ""
        return f"{self.message}{context_str}"


class ConfigurationError(TradingAPIError):
    """Raised when there are issues with configuration loading or validation."""

    pass


class BrokerConnectionError(TradingAPIError):
    """Raised when there are issues connecting to or communicating with brokers."""

    pass


class OrderError(TradingAPIError):
    """Raised when there are issues with order placement, modification, or cancellation."""

    pass


class MarketDataError(TradingAPIError):
    """Raised when there are issues retrieving market data or quotes."""

    pass


class ValidationError(TradingAPIError):
    """Raised when input validation fails."""

    pass


class NetworkError(TradingAPIError):
    """Raised when there are network-related issues."""

    pass


class AuthenticationError(TradingAPIError):
    """Raised when authentication fails with broker APIs."""

    pass


class RateLimitError(TradingAPIError):
    """Raised when API rate limits are exceeded."""

    pass


class DataError(TradingAPIError):
    """Raised when there are issues with data processing or formatting."""

    pass


class RedisError(TradingAPIError):
    """Raised when there are issues with Redis operations."""

    pass


class SymbolError(TradingAPIError):
    """Raised when there are issues with symbol mapping or validation."""

    pass


class CommissionError(TradingAPIError):
    """Raised when there are issues with commission calculations."""

    pass


class PnLError(TradingAPIError):
    """Raised when there are issues with P&L calculations."""

    pass


class ExpiryError(TradingAPIError):
    """Raised when there are issues with option expiry calculations."""

    pass


class MarginError(TradingAPIError):
    """Raised when there are issues with margin calculations."""

    pass


class TimeoutError(TradingAPIError):
    """Raised when operations timeout."""

    pass


class RetryableError(TradingAPIError):
    """Base class for errors that can be retried."""

    pass


class NonRetryableError(TradingAPIError):
    """Base class for errors that should not be retried."""

    pass


def is_retryable_error(error: Exception) -> bool:
    """Determine if an error is retryable."""
    if isinstance(error, RetryableError):
        return True
    if isinstance(error, NonRetryableError):
        return False

    # Default retryable errors
    retryable_types = (
        NetworkError,
        TimeoutError,
        RateLimitError,
        BrokerConnectionError,
    )

    return isinstance(error, retryable_types)


def add_error_context(error: Exception, context: Dict[str, Any]) -> Exception:
    """Add context to an existing exception."""
    if isinstance(error, TradingAPIError):
        error.context.update(context)
    return error


def create_error_context(**kwargs) -> Dict[str, Any]:
    """Create a standardized error context dictionary."""
    import datetime as dt
    import traceback

    context = {"timestamp": dt.datetime.now().isoformat(), "traceback": traceback.format_exc(), **kwargs}
    return context
