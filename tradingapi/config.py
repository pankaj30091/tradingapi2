import datetime as dt
import logging
import os

import yaml

# Singleton instance to ensure configuration is loaded once
_config_instance = None
logger = logging.getLogger(__name__)


class Config:
    def __init__(self, default_config_path):
        """
        Initialize the Config class.
        :param main_con
        fig_path: Path to the main YAML configuration file.
        """
        self.configs = {}
        self.commission_data = {}  # Preloaded commission data for all effective dates
        self.default_config_path = default_config_path
        self.custom_config_path = os.getenv("TRADINGAPI_CONFIG_PATH", None)
        self.config_path = self.custom_config_path or self.default_config_path
        if self.config_path:
            self.base_dir = os.path.dirname(self.config_path)  # Base directory for relative paths
            self.load_config(self.config_path)
            self.load_all_commissions()

    def load_config(self, config_file):
        """
        Load the main configuration file.
        :param config_file: Path to the main configuration file.
        """
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Configuration file '{config_file}' not found.")
        with open(config_file, "r") as file:
            try:
                self.configs = yaml.safe_load(file) or {}
            except yaml.YAMLError as exc:
                print(f"Error parsing YAML file: {exc}")
                raise

    def load_all_commissions(self):
        """
        Preload all commission files specified in the main configuration.
        """
        commissions = self.configs.get("commissions", [])
        for commission in commissions:
            effective_date = commission["effective_date"]
            relative_path = commission["file"]
            commission_file = os.path.join(self.base_dir, relative_path)

            if not os.path.exists(commission_file):
                raise FileNotFoundError(f"Commission file '{commission_file}' not found.")
            with open(commission_file, "r") as file:
                try:
                    self.commission_data[effective_date] = yaml.safe_load(file) or {}
                except yaml.YAMLError as exc:
                    print(f"Error parsing YAML file '{commission_file}': {exc}")
                    raise

    def get(self, key_path, default=None):
        """
        Get a value from the configuration using dot-separated key path.
        :param key_path: Dot-separated key path (e.g., 'FIVEPAISA.APP_NAME').
        :param default: Default value if the key path does not exist.
        :return: The value corresponding to the key path.
        """
        keys = key_path.split(".")
        value = self.configs
        try:
            for key in keys:
                value = value[key]
            return value
        except KeyError:
            return default

    def get_commission_by_date(self, target_date, key_path, default=None):
        """
        Get a value from the commission data for a specific target date using dot-separated key path.
        :param target_date: The date for which the commission data is needed (YYYY-MM-DD).
        :param key_path: Dot-separated key path (e.g., 'SHOONYA.FUT.BUY.flat').
        :param default: Default value if the key path does not exist.
        :return: The value corresponding to the key path.
        """
        if target_date == "":
            return 0
        target_date = dt.datetime.strptime(target_date, "%Y-%m-%d").date()
        applicable_commission = None
        # Find the most recent commission file before or on the target date
        for effective_date in sorted(self.commission_data.keys(), reverse=True):
            effective_date_obj = dt.datetime.strptime(effective_date, "%Y-%m-%d").date()
            if target_date >= effective_date_obj:
                applicable_commission = self.commission_data[effective_date]
                break

        if not applicable_commission:
            raise ValueError(f"No commission data found for the date {target_date}")

        # Traverse the dot-separated key path
        keys = key_path.split(".")
        value = applicable_commission
        try:
            for key in keys:
                value = value[key]
            return value
        except KeyError:
            return default


# Global functions for managing configuration


def load_config(default_config_path):
    """
    Load the configuration globally.
    :param config_file_path: Path to the main configuration file.
    """
    global _config_instance
    _config_instance = Config(default_config_path)
    logger.warn(f"Config loaded from file {_config_instance.config_path}")


def is_config_loaded():
    """
    Check if the configuration is already loaded.
    :return: True if the configuration is loaded, otherwise False.
    """
    return _config_instance is not None


def get_config():
    """
    Retrieve the loaded configuration instance.
    :return: Config instance if loaded, otherwise raises ValueError.
    """
    if not is_config_loaded():
        raise ValueError("Configuration has not been loaded yet.")
    return _config_instance


# Example usage
if __name__ == "__main__":
    # Initialize configuration with the main YAML file
    config_file_path = "tradingapi/config/config.yaml"  # Replace with the actual path
    config = Config(config_file_path)

    # Access global configurations
    timezone = config.get("tz")
    data_path = config.get("datapath")
    market_open_time = config.get("market_open_time")

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

    print(f"SHOONYA GST (Entry Date): {shoonya_gst_entry}")
    print(f"SHOONYA GST (Exit Date): {shoonya_gst_exit}")
    print(f"SHOONYA FUT BUY Flat (Entry Date): {shoonya_fut_buy_flat_entry}")
    print(f"FIVEPAISA OPT SHORT Exchange (Exit Date): {fivepaisa_opt_short_exchange_exit}")
