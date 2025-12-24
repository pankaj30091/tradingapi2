# tradingAPI/__init__.py
import inspect
import logging
import os
import signal
import sys
from importlib.resources import files
from logging.handlers import TimedRotatingFileHandler
from typing import Optional, List

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from .config import is_config_loaded, load_config


class StructuredFormatter(logging.Formatter):
    """Custom formatter that includes extra fields in the log output."""

    def format(self, record):
        # Create a copy of the record to avoid modifying the original
        record_copy = logging.LogRecord(
            name=record.name,
            level=record.levelno,
            pathname=record.pathname,
            lineno=record.lineno,
            msg=record.msg,
            args=record.args,
            exc_info=record.exc_info,
            func=record.funcName,
        )

        # Copy all attributes from the original record
        for key, value in record.__dict__.items():
            if key not in record_copy.__dict__:
                setattr(record_copy, key, value)

        # Add extra fields to the message if they exist
        extra_parts = []

        # Check for extra fields in the record (excluding standard logging fields)
        standard_fields = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "getMessage",
            "exc_info",
            "exc_text",
            "stack_info",
            "asctime",
            "message",
        }

        for key, value in record_copy.__dict__.items():
            if key not in standard_fields and not key.startswith("_"):
                extra_parts.append(f"{key}={value}")

        if extra_parts:
            record_copy.msg = f"{record_copy.msg} | {' | '.join(extra_parts)}"

        return super().format(record_copy)


def get_default_config_path():
    """Returns the path to the default config file included in the package."""
    return files("tradingapi").joinpath("config/config.yaml")


class TradingAPILogger:
    """Centralized logger for the tradingAPI package with structured logging capabilities."""

    def __init__(self, name: str = "tradingapi"):
        self.logger = logging.getLogger(name)
        self._configured = False

    def configure(
        self,
        level: int = logging.WARNING,
        log_file: Optional[str] = None,
        clear_existing_handlers: bool = False,
        enable_console: bool = True,
        backup_count: int = 7,
        format_string: str = "%(asctime)s:%(name)s:%(filename)s:%(lineno)s - %(funcName)20s() ] %(levelname)s: %(message)s",
        enable_structured_logging: bool = True,
    ):
        """
        Configure logging for the tradingAPI package with enhanced features.

        Args:
            level: Logging level (e.g., logging.DEBUG, logging.INFO).
            log_file: Path to the log file. If None, logs will go to the console.
            clear_existing_handlers: Whether to clear existing handlers from the root logger.
            enable_console: Whether console logging is enabled.
            backup_count: Number of log files to keep.
            format_string: Custom format string for log messages.
            enable_structured_logging: Enable structured logging with additional context.
        """
        if self._configured and not clear_existing_handlers:
            return

        if clear_existing_handlers:
            for handler in logging.root.handlers[:]:
                logging.root.removeHandler(handler)
            for handler in self.logger.handlers[:]:
                self.logger.removeHandler(handler)

        # Create handlers
        handlers = []
        # Use StructuredFormatter for better extra field handling
        formatter = StructuredFormatter(format_string)

        if log_file:
            # Ensure log directory exists
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)

            file_handler = TimedRotatingFileHandler(log_file, when="midnight", backupCount=backup_count)
            file_handler.suffix = "%Y%m%d"
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)

        if enable_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            handlers.append(console_handler)

        # Configure the logger
        self.logger.setLevel(level)
        for handler in handlers:
            self.logger.addHandler(handler)

        self._configured = True

        # Log configuration
        self.logger.info(
            "TradingAPI logging configured",
            extra={
                "log_file": log_file,
                "level": logging.getLevelName(level),
                "console_enabled": enable_console,
                "structured_logging": enable_structured_logging,
            },
        )

    def get_logger(self, name: str = None) -> logging.Logger:
        """Get a logger instance with the specified name."""
        if name:
            return logging.getLogger(f"tradingapi.{name}")
        return self.logger

    def _get_caller_info(self):
        """Get information about the calling function."""
        try:
            # Get the caller frame (skip this method and the logging method)
            caller_frame = inspect.currentframe().f_back.f_back
            if caller_frame:
                info = inspect.getframeinfo(caller_frame)
                return {
                    "caller_filename": info.filename,
                    "caller_lineno": info.lineno,
                    "caller_function": info.function,
                }
        except Exception:
            pass
        return {}

    def log_error(self, message: str, error: Exception = None, context: dict = None, exc_info: bool = True):
        """Log an error with structured context."""
        extra = context or {}
        if error:
            extra["error_type"] = type(error).__name__
            extra["error_message"] = str(error)

        # Add caller information
        caller_info = self._get_caller_info()
        extra.update(caller_info)

        self.logger.error(message, extra=extra, exc_info=exc_info)

    def log_warning(self, message: str, context: dict = None):
        """Log a warning with structured context."""
        extra = context or {}

        # Add caller information
        caller_info = self._get_caller_info()
        extra.update(caller_info)

        self.logger.warning(message, extra=extra)

    def log_info(self, message: str, context: dict = None):
        """Log an info message with structured context."""
        extra = context or {}

        # Add caller information
        caller_info = self._get_caller_info()
        extra.update(caller_info)

        self.logger.info(message, extra=extra)

    def log_debug(self, message: str, context: dict = None):
        """Log a debug message with structured context."""
        extra = context or {}

        # Add caller information
        caller_info = self._get_caller_info()
        extra.update(caller_info)

        self.logger.debug(message, extra=extra)


# Global logger instance
trading_logger = TradingAPILogger()


def configure_logging(
    module_names: Optional[List[str]] = None,
    level: int = logging.WARNING,
    log_file: Optional[str] = None,
    clear_existing_handlers: bool = False,
    enable_console: bool = True,
    backup_count: int = 7,
    format_string: str = "%(asctime)s:%(name)s:%(filename)s:%(lineno)s - %(funcName)20s() ] %(levelname)s: %(message)s",
    enable_structured_logging: bool = True,
    configure_root_logger: bool = False,
):
    """
    Configure logging for specific modules or all modules in the tradingAPI package.

    Args:
        module_names: List of module names to enable logging for. If None, configure logging for all modules.
        level: Logging level (e.g., logging.DEBUG, logging.INFO).
        log_file: Path to the log file. If None, logs will go to the console.
        clear_existing_handlers: Whether to clear existing handlers from the root logger.
        enable_console: Whether console logging is enabled.
        backup_count: Number of log files to keep.
        format_string: Custom format string for log messages.
        enable_structured_logging: Enable structured logging with additional context.
        configure_root_logger: Whether to configure the root logger (can cause duplicate logs if True).
    """
    trading_logger.configure(
        level=level,
        log_file=log_file,
        clear_existing_handlers=clear_existing_handlers,
        enable_console=enable_console,
        backup_count=backup_count,
        format_string=format_string,
        enable_structured_logging=enable_structured_logging,
    )

    # Only configure root logger if explicitly requested to avoid duplicate logs
    if configure_root_logger:
        # Configure root logger to capture all errors
        root_logger = logging.getLogger()
        root_logger.setLevel(level)

        # Clear existing handlers if requested
        if clear_existing_handlers:
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)

        # Create handlers for root logger
        handlers = []
        # Use StructuredFormatter for better extra field handling
        formatter = StructuredFormatter(format_string)

        if log_file:
            # Ensure log directory exists
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)

            file_handler = TimedRotatingFileHandler(log_file, when="midnight", backupCount=backup_count)
            file_handler.suffix = "%Y%m%d"
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)

        if enable_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            handlers.append(console_handler)

        # Add handlers to root logger
        for handler in handlers:
            root_logger.addHandler(handler)

    # Configure specific modules if requested
    if module_names:
        for module_name in module_names:
            logger = logging.getLogger(module_name)
            logger.setLevel(level)
            # Don't add handlers to avoid duplication


# Set default logging level to WARNING and log to console by default
# Don't configure root logger by default to avoid duplicate logs
configure_logging(configure_root_logger=False)


def initialize_config(config_file_path: str, force_reload: bool = True):
    """Initialize configuration with enhanced error handling."""
    try:
        if is_config_loaded() and not force_reload:
            raise RuntimeError("Configuration is already loaded.")
        else:
            load_config(config_file_path)
            trading_logger.log_info(
                "Configuration initialized successfully",
                {"config_file": config_file_path, "force_reload": force_reload},
            )
    except Exception as e:
        trading_logger.log_error(
            "Failed to initialize configuration", e, {"config_file": config_file_path, "force_reload": force_reload}
        )
        raise


initialize_config(get_default_config_path())


def enable_runtime_log_level_toggle(enable: bool = True):
    """
    Enable runtime log level toggling via SIGUSR1 signal.
    
    When enabled, sending SIGUSR1 to the process will toggle between
    DEBUG and INFO logging levels.
    
    This implementation supports handler chaining - if another package
    has already registered a SIGUSR1 handler, both handlers will execute
    (this handler runs first, then calls the previous handler).
    
    Args:
        enable: If True, register the signal handler. If False, remove it.
    """
    def toggle_debug(signum, frame):
        current_level = trading_logger.logger.level
        if current_level == logging.DEBUG:
            # Change to INFO
            trading_logger.logger.setLevel(logging.INFO)
            # Also update all handlers
            for handler in trading_logger.logger.handlers:
                handler.setLevel(logging.INFO)
            trading_logger.log_info(
                "Log level changed to INFO via signal",
                {"signal": "SIGUSR1", "previous_level": "DEBUG"}
            )
        else:
            # Change to DEBUG
            trading_logger.logger.setLevel(logging.DEBUG)
            # Also update all handlers
            for handler in trading_logger.logger.handlers:
                handler.setLevel(logging.DEBUG)
            trading_logger.log_info(
                "Log level changed to DEBUG via signal",
                {"signal": "SIGUSR1", "previous_level": logging.getLevelName(current_level)}
            )
    
    if enable:
        try:
            # Get the current handler (if any) before registering ours
            current_handler = signal.signal(signal.SIGUSR1, toggle_debug)
            
            # If there was a previous handler, chain it
            if current_handler not in (signal.SIG_DFL, signal.SIG_IGN, None):
                def chained_handler(signum, frame):
                    # Execute our handler first
                    toggle_debug(signum, frame)
                    # Then call the previous handler if it's callable
                    if callable(current_handler):
                        try:
                            current_handler(signum, frame)
                        except Exception as e:
                            # Log but don't fail if previous handler has issues
                            trading_logger.log_warning(
                                "Error in chained SIGUSR1 handler",
                                {"error": str(e), "error_type": type(e).__name__}
                            )
                
                # Register the chained handler
                signal.signal(signal.SIGUSR1, chained_handler)
                trading_logger.log_info(
                    "Runtime log level toggle enabled (chained with existing handler)",
                    {
                        "signal": "SIGUSR1",
                        "usage": "kill -SIGUSR1 <pid> to toggle DEBUG/INFO",
                        "previous_handler": str(current_handler)
                    }
                )
            else:
                trading_logger.log_info(
                    "Runtime log level toggle enabled",
                    {"signal": "SIGUSR1", "usage": "kill -SIGUSR1 <pid> to toggle DEBUG/INFO"}
                )
        except (ValueError, OSError) as e:
            # Signal might not be available on all platforms
            trading_logger.log_warning(
                "Could not register signal handler for log level toggle",
                {"error": str(e), "platform": sys.platform}
            )
    else:
        # Restore default handler
        try:
            signal.signal(signal.SIGUSR1, signal.SIG_DFL)
        except (ValueError, OSError):
            pass


# Automatically enable runtime log level toggle
enable_runtime_log_level_toggle(enable=True)
