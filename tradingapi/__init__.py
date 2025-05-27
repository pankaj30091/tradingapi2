# tradingAPI/__init__.py
import logging
import os
import sys
from importlib.resources import files
from logging.handlers import TimedRotatingFileHandler

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from .config import is_config_loaded, load_config


def get_default_config_path():
    """Returns the path to the default config file included in the package."""
    return files("tradingapi").joinpath("config/config.yaml")


def configure_logging(
    module_names=None,
    level=logging.WARNING,
    log_file=None,
    clear_existing_handlers=False,
    enable_console=True,
    backup_count=7,
    format_string="%(asctime)s:%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s",
):
    """
    Configure logging for specific modules or all modules in the tradingAPI package.

    Args:
        module_names (list of str): List of module names to enable logging for. If None, configure logging for all modules.
        level (int): Logging level (e.g., logging.DEBUG, logging.INFO).
        log_file (str): Path to the log file. If None, logs will go to the console.
        clear_existing_handlers (bool): Whether to clear existing handlers from the root logger.
        enable_console (bool): Whether console logging is enabled
        backup_count (int): number of log files to keep
    """
    if clear_existing_handlers:
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

    # Create handlers
    handlers = []
    formatter = logging.Formatter(format_string)
    if log_file:
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

    # Configure logging for specific modules or globally
    if module_names:
        for module_name in module_names:
            logger = logging.getLogger(module_name)
            logger.setLevel(level)
            for handler in handlers:
                logger.addHandler(handler)
    else:
        # Configure root logger if no specific modules are mentioned
        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        for handler in handlers:
            root_logger.addHandler(handler)


# # Set default logging level to WARNING and log to console by default
configure_logging()


def initialize_config(config_file_path: str, force_reload=True):
    if is_config_loaded() and not force_reload:
        raise RuntimeError("Configuration is already loaded.")
    else:
        load_config(config_file_path)


initialize_config(get_default_config_path())
