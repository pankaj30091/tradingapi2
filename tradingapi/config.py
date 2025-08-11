import datetime as dt
import logging
import os
import sys
from typing import Any, Dict, Optional, Union
from pathlib import Path

import yaml

# Import exceptions without trading_logger to avoid circular import
from .exceptions import ConfigurationError, ValidationError, create_error_context


# Import trading_logger lazily to avoid circular import
def get_trading_logger():
    """Get trading_logger instance to avoid circular imports."""
    from . import trading_logger

    return trading_logger


# Singleton instance to ensure configuration is loaded once
_config_instance = None


class Config:
    """
    Robust configuration management class with enhanced logging and error handling.

    Features:
    - Structured logging with context
    - Comprehensive error handling
    - Configuration validation
    - Commission data management
    - Environment variable support
    - Type safety and validation
    """

    def __init__(self, default_config_path: str):
        """
        Initialize the Config class with enhanced error handling and logging.

        Args:
            default_config_path: Path to the main YAML configuration file.

        Raises:
            ConfigurationError: If configuration initialization fails
            ValidationError: If configuration validation fails
            FileNotFoundError: If configuration file is not found
        """
        try:
            self.configs: Dict[str, Any] = {}
            self.commission_data: Dict[str, Dict[str, Any]] = {}
            self.default_config_path = Path(default_config_path)
            self.custom_config_path = self._get_custom_config_path()
            self.config_path = self.custom_config_path or self.default_config_path

            get_trading_logger().log_info(
                "Initializing configuration",
                {
                    "default_path": str(self.default_config_path),
                    "custom_path": str(self.custom_config_path) if self.custom_config_path else None,
                    "final_path": str(self.config_path),
                },
            )

            if self.config_path:
                self.base_dir = self.config_path.parent
                self._validate_config_path()
                self.load_config(self.config_path)
                self._validate_config_structure()
                self.load_all_commissions()
                self._validate_commission_data()

                get_trading_logger().log_info(
                    "Configuration initialized successfully",
                    {
                        "config_path": str(self.config_path),
                        "commission_files": len(self.commission_data),
                        "config_keys": list(self.configs.keys()),
                    },
                )
            else:
                raise ConfigurationError(
                    "No valid configuration path found",
                    create_error_context(
                        default_path=str(self.default_config_path),
                        custom_path=str(self.custom_config_path) if self.custom_config_path else None,
                    ),
                )

        except Exception as e:
            context = create_error_context(
                default_path=str(self.default_config_path),
                custom_path=str(self.custom_config_path) if self.custom_config_path else None,
                error=str(e),
            )
            get_trading_logger().log_error("Configuration initialization failed", e, context)
            raise ConfigurationError(f"Failed to initialize configuration: {str(e)}", context)

    def _get_custom_config_path(self) -> Optional[Path]:
        """Get custom configuration path from environment variable."""
        custom_path = os.getenv("TRADINGAPI_CONFIG_PATH")
        if custom_path:
            path = Path(custom_path)
            if path.exists():
                return path
            else:
                get_trading_logger().log_warning("Custom config path does not exist", {"custom_path": str(path)})
        return None

    def _validate_config_path(self) -> None:
        """Validate that the configuration file exists and is readable."""
        if not self.config_path.exists():
            context = create_error_context(config_path=str(self.config_path), base_dir=str(self.base_dir))
            raise FileNotFoundError(f"Configuration file '{self.config_path}' not found.", context)

        if not os.access(self.config_path, os.R_OK):
            context = create_error_context(config_path=str(self.config_path), permissions="read")
            raise PermissionError(f"Cannot read configuration file '{self.config_path}'", context)

    def _validate_config_structure(self) -> None:
        """Validate the basic structure of the configuration."""
        required_keys = ["tz", "datapath", "market_open_time"]
        missing_keys = [key for key in required_keys if key not in self.configs]

        if missing_keys:
            context = create_error_context(missing_keys=missing_keys, available_keys=list(self.configs.keys()))
            raise ValidationError(f"Missing required configuration keys: {missing_keys}", context)

    def _validate_commission_data(self) -> None:
        """Validate commission data structure."""
        if not self.commission_data:
            get_trading_logger().log_warning("No commission data loaded", {"config_path": str(self.config_path)})
            return

        for effective_date, commission_data in self.commission_data.items():
            if not isinstance(commission_data, dict):
                context = create_error_context(effective_date=effective_date, data_type=type(commission_data).__name__)
                raise ValidationError(f"Invalid commission data structure for date {effective_date}", context)

    def load_config(self, config_file: Path) -> None:
        """
        Load the main configuration file with enhanced error handling.

        Args:
            config_file: Path to the main configuration file.

        Raises:
            ConfigurationError: If configuration loading fails
            ValidationError: If configuration structure is invalid
        """
        try:
            get_trading_logger().log_debug("Loading configuration file", {"config_file": str(config_file)})

            with open(config_file, "r", encoding="utf-8") as file:
                try:
                    self.configs = yaml.safe_load(file) or {}
                except yaml.YAMLError as exc:
                    context = create_error_context(config_file=str(config_file), yaml_error=str(exc))
                    raise ConfigurationError(f"Error parsing YAML file: {exc}", context)

            if not isinstance(self.configs, dict):
                context = create_error_context(config_file=str(config_file), config_type=type(self.configs).__name__)
                raise ValidationError("Configuration must be a dictionary", context)

            get_trading_logger().log_info(
                "Configuration file loaded successfully",
                {"config_file": str(config_file), "config_keys": list(self.configs.keys())},
            )

        except Exception as e:
            context = create_error_context(config_file=str(config_file), error=str(e))
            get_trading_logger().log_error("Failed to load configuration file", e, context)
            raise

    def load_all_commissions(self) -> None:
        """
        Preload all commission files with enhanced error handling and validation.

        Raises:
            ConfigurationError: If commission loading fails
            ValidationError: If commission data structure is invalid
        """
        try:
            commissions = self.configs.get("commissions", [])

            if not commissions:
                get_trading_logger().log_warning(
                    "No commission files specified in configuration", {"config_keys": list(self.configs.keys())}
                )
                return

            get_trading_logger().log_debug("Loading commission files", {"commission_count": len(commissions)})

            for commission in commissions:
                try:
                    self._load_single_commission(commission)
                except Exception as e:
                    context = create_error_context(commission=commission, error=str(e))
                    get_trading_logger().log_error("Failed to load commission file", e, context)
                    raise ConfigurationError(f"Failed to load commission: {str(e)}", context)

            get_trading_logger().log_info(
                "All commission files loaded successfully",
                {"commission_count": len(self.commission_data), "effective_dates": list(self.commission_data.keys())},
            )

        except Exception as e:
            context = create_error_context(commission_count=len(self.configs.get("commissions", [])), error=str(e))
            get_trading_logger().log_error("Failed to load commission files", e, context)
            raise

    def _load_single_commission(self, commission: Dict[str, Any]) -> None:
        """Load a single commission file with validation."""
        if not isinstance(commission, dict):
            raise ValidationError(
                "Commission entry must be a dictionary", create_error_context(commission_type=type(commission).__name__)
            )

        effective_date = commission.get("effective_date")
        relative_path = commission.get("file")

        if not effective_date or not relative_path:
            raise ValidationError(
                "Commission entry must have 'effective_date' and 'file' keys",
                create_error_context(commission=commission),
            )

        commission_file = self.base_dir / relative_path

        if not commission_file.exists():
            raise FileNotFoundError(
                f"Commission file '{commission_file}' not found.",
                create_error_context(commission_file=str(commission_file), base_dir=str(self.base_dir)),
            )

        with open(commission_file, "r", encoding="utf-8") as file:
            try:
                commission_data = yaml.safe_load(file) or {}
                if not isinstance(commission_data, dict):
                    raise ValidationError(
                        "Commission data must be a dictionary",
                        create_error_context(
                            commission_file=str(commission_file), data_type=type(commission_data).__name__
                        ),
                    )
                self.commission_data[effective_date] = commission_data

                get_trading_logger().log_debug(
                    "Commission file loaded",
                    {
                        "effective_date": effective_date,
                        "commission_file": str(commission_file),
                        "data_keys": list(commission_data.keys()),
                    },
                )

            except yaml.YAMLError as exc:
                raise ConfigurationError(
                    f"Error parsing commission YAML file '{commission_file}': {exc}",
                    create_error_context(commission_file=str(commission_file), yaml_error=str(exc)),
                )

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get a value from the configuration using dot-separated key path with validation.

        Args:
            key_path: Dot-separated key path (e.g., 'FIVEPAISA.APP_NAME').
            default: Default value if the key path does not exist.

        Returns:
            The value corresponding to the key path or default value.

        Raises:
            ValidationError: If key_path is invalid
        """
        try:
            if not isinstance(key_path, str) or not key_path.strip():
                raise ValidationError(
                    "Key path must be a non-empty string",
                    create_error_context(key_path=key_path, key_path_type=type(key_path).__name__),
                )

            keys = key_path.split(".")
            value = self.configs

            for key in keys:
                if not isinstance(value, dict):
                    get_trading_logger().log_warning(
                        "Invalid key path - not a dictionary",
                        {"key_path": key_path, "current_key": key, "value_type": type(value).__name__},
                    )
                    return default

                if key not in value:
                    get_trading_logger().log_debug(
                        "Key not found in configuration",
                        {"key_path": key_path, "missing_key": key, "available_keys": list(value.keys())},
                    )
                    return default

                value = value[key]

            return value

        except Exception as e:
            get_trading_logger().log_error(
                "Error getting configuration value", e, {"key_path": key_path, "default": default}
            )
            return default

    def get_commission_by_date(self, target_date: str, key_path: str, default: Any = None) -> Union[float, int]:
        """
        Get commission value for a specific date with enhanced validation and error handling.

        Args:
            target_date: The date for which commission data is needed (YYYY-MM-DD).
            key_path: Dot-separated key path (e.g., 'SHOONYA.FUT.BUY.flat').
            default: Default value if the key path does not exist.

        Returns:
            The commission value for the specified date and key path.

        Raises:
            ValidationError: If date format or key path is invalid
            ConfigurationError: If no commission data is available for the date
        """
        try:
            # Handle empty date case
            if not target_date or target_date.strip() == "":
                get_trading_logger().log_debug(
                    "Empty target date provided", {"target_date": target_date, "key_path": key_path}
                )
                return 0

            # Validate date format
            try:
                target_date_obj = dt.datetime.strptime(target_date, "%Y-%m-%d").date()
            except ValueError as e:
                raise ValidationError(
                    f"Invalid date format '{target_date}'. Expected YYYY-MM-DD",
                    create_error_context(target_date=target_date, expected_format="YYYY-MM-DD"),
                )

            # Validate key path
            if not isinstance(key_path, str) or not key_path.strip():
                raise ValidationError(
                    "Key path must be a non-empty string",
                    create_error_context(key_path=key_path, key_path_type=type(key_path).__name__),
                )

            # Find applicable commission data
            applicable_commission = self._find_applicable_commission(target_date_obj)

            if not applicable_commission:
                context = create_error_context(
                    target_date=target_date, available_dates=list(self.commission_data.keys())
                )
                raise ConfigurationError(f"No commission data found for date {target_date}", context)

            # Get value from commission data
            value = self._get_value_from_dict(applicable_commission, key_path, default)

            # Validate that the value is numeric
            if value is not None and not isinstance(value, (int, float)):
                get_trading_logger().log_warning(
                    "Commission value is not numeric",
                    {
                        "target_date": target_date,
                        "key_path": key_path,
                        "value": value,
                        "value_type": type(value).__name__,
                    },
                )
                return default or 0

            get_trading_logger().log_debug(
                "Commission value retrieved",
                {
                    "target_date": target_date,
                    "key_path": key_path,
                    "value": value,
                    "effective_date": self._get_effective_date(target_date_obj),
                },
            )

            return value or 0

        except Exception as e:
            get_trading_logger().log_error(
                "Error getting commission value",
                e,
                {"target_date": target_date, "key_path": key_path, "default": default},
            )
            return default or 0

    def _find_applicable_commission(self, target_date: dt.date) -> Optional[Dict[str, Any]]:
        """Find the most recent commission data before or on the target date."""
        applicable_commission = None
        applicable_date = None

        for effective_date in sorted(self.commission_data.keys(), reverse=True):
            try:
                effective_date_obj = dt.datetime.strptime(effective_date, "%Y-%m-%d").date()
                if target_date >= effective_date_obj:
                    applicable_commission = self.commission_data[effective_date]
                    applicable_date = effective_date
                    break
            except ValueError as e:
                get_trading_logger().log_warning(
                    "Invalid effective date format", {"effective_date": effective_date, "error": str(e)}
                )
                continue

        if applicable_commission:
            get_trading_logger().log_debug(
                "Found applicable commission",
                {
                    "target_date": target_date.isoformat(),
                    "effective_date": applicable_date,
                    "commission_keys": list(applicable_commission.keys()),
                },
            )

        return applicable_commission

    def _get_value_from_dict(self, data: Dict[str, Any], key_path: str, default: Any = None) -> Any:
        """Get value from dictionary using dot-separated key path."""
        keys = key_path.split(".")
        value = data

        try:
            for key in keys:
                if not isinstance(value, dict):
                    get_trading_logger().log_debug(
                        "Invalid key path - not a dictionary",
                        {"key_path": key_path, "current_key": key, "value_type": type(value).__name__},
                    )
                    return default

                if key not in value:
                    get_trading_logger().log_debug(
                        "Key not found in commission data",
                        {"key_path": key_path, "missing_key": key, "available_keys": list(value.keys())},
                    )
                    return default

                value = value[key]

            return value

        except Exception as e:
            get_trading_logger().log_error(
                "Error traversing commission data", e, {"key_path": key_path, "data_keys": list(data.keys())}
            )
            return default

    def _get_effective_date(self, target_date: dt.date) -> Optional[str]:
        """Get the effective date for a given target date."""
        for effective_date in sorted(self.commission_data.keys(), reverse=True):
            try:
                effective_date_obj = dt.datetime.strptime(effective_date, "%Y-%m-%d").date()
                if target_date >= effective_date_obj:
                    return effective_date
            except ValueError:
                continue
        return None


# Global functions for managing configuration with enhanced error handling


def load_config(default_config_path: str) -> None:
    """
    Load the configuration globally with enhanced error handling and logging.

    Args:
        default_config_path: Path to the main configuration file.

    Raises:
        ConfigurationError: If configuration loading fails
    """
    global _config_instance

    try:
        get_trading_logger().log_info("Loading global configuration", {"config_path": default_config_path})

        _config_instance = Config(default_config_path)

        get_trading_logger().log_info(
            "Global configuration loaded successfully",
            {
                "config_path": str(_config_instance.config_path),
                "commission_files": len(_config_instance.commission_data),
            },
        )

    except Exception as e:
        context = create_error_context(config_path=default_config_path, error=str(e))
        get_trading_logger().log_error("Failed to load global configuration", e, context)
        raise ConfigurationError(f"Failed to load global configuration: {str(e)}", context)


def is_config_loaded() -> bool:
    """
    Check if the configuration is already loaded.

    Returns:
        True if the configuration is loaded, otherwise False.
    """
    return _config_instance is not None


def get_config() -> Config:
    """
    Retrieve the loaded configuration instance with validation.

    Returns:
        Config instance if loaded.

    Raises:
        ConfigurationError: If configuration has not been loaded.
    """
    if not is_config_loaded():
        context = create_error_context(
            config_instance_type=type(_config_instance).__name__ if _config_instance else None
        )
        raise ConfigurationError("Configuration has not been loaded yet.", context)

    return _config_instance


# Example usage with enhanced error handling
if __name__ == "__main__":
    try:
        # Initialize configuration with the main YAML file
        config_file_path = "tradingapi/config/config.yaml"  # Replace with the actual path
        config = Config(config_file_path)

        # Access global configurations
        timezone = config.get("tz")
        data_path = config.get("datapath")
        market_open_time = config.get("market_open_time")

        get_trading_logger().log_info(
            "Configuration test completed",
            {"timezone": timezone, "data_path": data_path, "market_open_time": market_open_time},
        )

        print(f"Timezone: {timezone}")
        print(f"Data Path: {data_path}")
        print(f"Market Open Time: {market_open_time}")

        # Query commission data for specific dates
        entry_date = "2024-10-01"
        exit_date = "2024-12-05"

        shoonya_gst_entry = config.get_commission_by_date(entry_date, "SHOONYA.GST")
        shoonya_gst_exit = config.get_commission_by_date(exit_date, "SHOONYA.GST")

        shoonya_fut_buy_flat_entry = config.get_commission_by_date(entry_date, "SHOONYA.FUT.BUY.flat")
        fivepaisa_opt_short_exchange_exit = config.get_commission_by_date(
            exit_date, "FIVEPAISA.OPT.SHORT.percentage.exchange"
        )

        get_trading_logger().log_info(
            "Commission test completed",
            {
                "entry_date": entry_date,
                "exit_date": exit_date,
                "shoonya_gst_entry": shoonya_gst_entry,
                "shoonya_gst_exit": shoonya_gst_exit,
                "shoonya_fut_buy_flat_entry": shoonya_fut_buy_flat_entry,
                "fivepaisa_opt_short_exchange_exit": fivepaisa_opt_short_exchange_exit,
            },
        )

        print(f"SHOONYA GST (Entry Date): {shoonya_gst_entry}")
        print(f"SHOONYA GST (Exit Date): {shoonya_gst_exit}")
        print(f"SHOONYA FUT BUY Flat (Entry Date): {shoonya_fut_buy_flat_entry}")
        print(f"FIVEPAISA OPT SHORT Exchange (Exit Date): {fivepaisa_opt_short_exchange_exit}")

    except Exception as e:
        get_trading_logger().log_error("Configuration test failed", e, {"script": "config_test"})
        sys.exit(1)
