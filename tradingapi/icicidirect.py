import datetime as dt
import io
import inspect
import json
import logging
import math
import os
import secrets
import subprocess
import sys
import time
import traceback
import zipfile
from typing import Dict, List, Optional, Union, cast

import pandas as pd
import redis
import requests
from breeze_connect import BreezeConnect

# Re-enable IPv6 after breeze_connect import (breeze_connect sets HAS_IPV6 = False at load).
try:
    requests.packages.urllib3.util.connection.HAS_IPV6 = True  # type: ignore[attr-defined]
except Exception:
    try:
        import urllib3.util.connection as _conn
        _conn.HAS_IPV6 = True
    except Exception:
        pass

from .broker_base import (
    BrokerBase,
    Brokers,
    HistoricalData,
    Order,
    OrderInfo,
    OrderStatus,
    Position,
    Price,
    _normalize_as_of_date,
)
from .config import get_config
from .utils import set_starting_internal_ids_int, update_order_status
from .exceptions import (
    AuthenticationError,
    BrokerConnectionError,
    ConfigurationError,
    DataError,
    MarketDataError,
    OrderError,
    SymbolError,
    TradingAPIError,
    ValidationError,
    create_error_context,
)
from .error_handling import retry_on_error, safe_execute, log_execution_time, validate_inputs
from . import trading_logger
from . import globals as tradingapi_globals
from .globals import get_tradingapi_now
from chameli.dateutils import format_datetime

logger = logging.getLogger(__name__)
config = get_config()


def _temporarily_force_ipv4():
    """
    Force IPv4 for the next request(s) only (ICICIDirect/Breeze endpoints can return 403 over IPv6).
    Returns the previous HAS_IPV6 value so caller can restore in a finally.
    """
    try:
        conn = requests.packages.urllib3.util.connection  # type: ignore[attr-defined]
        old = getattr(conn, "HAS_IPV6", True)
        conn.HAS_IPV6 = False
        return old
    except Exception:
        try:
            import urllib3.util.connection as conn
            old = getattr(conn, "HAS_IPV6", True)
            conn.HAS_IPV6 = False
            return old
        except Exception:
            return None


def _restore_ipv6(previous):
    """Restore HAS_IPV6 to previous value (from _temporarily_force_ipv4)."""
    if previous is None:
        return
    try:
        requests.packages.urllib3.util.connection.HAS_IPV6 = previous  # type: ignore[attr-defined]
    except Exception:
        try:
            import urllib3.util.connection as conn
            conn.HAS_IPV6 = previous
        except Exception:
            pass


@log_execution_time
@retry_on_error(max_retries=3, delay=2.0, backoff_factor=2.0)
def save_symbol_data(saveToFolder: bool = True) -> pd.DataFrame:
    """
    Download and process ICICIDirect/Breeze symbol master into a standard
    long_symbol-based CSV layout similar to other brokers.

    This implementation is intentionally conservative and focuses on
    cash/index instruments. Derivatives can be added later once the
    exact file structure is finalized.
    """
    bhavcopyfolder = config.get("bhavcopy_folder")
    url = config.get("ICICIDIRECT.SYMBOL_MASTER_URL")

    if not bhavcopyfolder:
        raise ConfigurationError(
            "Missing 'bhavcopy_folder' in config for IciciDirect symbol data",
            create_error_context(),
        )

    if not url:
        raise ConfigurationError(
            "Missing 'ICICIDIRECT.SYMBOL_MASTER_URL' in config for IciciDirect symbol data",
            create_error_context(),
        )

    dest_file = f"{bhavcopyfolder}/{dt.datetime.today().strftime('%Y%m%d')}_icicidirect_instruments.csv"

    _proxies = None
    try:
        from .proxy_utils import get_proxies_for_broker
        _proxies = get_proxies_for_broker("ICICIDIRECT")
    except Exception:
        pass
    _prev_ipv6 = _temporarily_force_ipv4()
    try:
        response = requests.get(url, allow_redirects=True, timeout=60, proxies=_proxies or {})
    finally:
        _restore_ipv6(_prev_ipv6)
    try:
        if response.status_code != 200:
            raise DataError(
                f"Failed to fetch IciciDirect symbol data. Status code: {response.status_code}",
                create_error_context(status_code=response.status_code, url=url),
            )

        # The SecurityMaster is a ZIP; read specific inner files for NSE/BSE cash and F&O.
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            all_names = set(zf.namelist())
            wanted_files = [
                "NSEScripMaster.txt",   # NSE cash
                "BSEScripMaster.txt",   # BSE cash
                "FONSEScripMaster.txt", # NSE F&O
                "FOBSEScripMaster.txt", # BSE F&O
            ]

            parts: list[pd.DataFrame] = []

            for fname in wanted_files:
                if fname not in all_names:
                    continue

                with zf.open(fname) as f:
                    df = pd.read_csv(f)
                # Clean up column names: trim whitespace and surrounding quotes
                df.columns = [c.strip().strip('"') for c in df.columns]

                trading_logger.log_debug(
                    "IciciDirect raw symbol master slice loaded",
                    {"file": fname, "shape": df.shape, "columns": list(df.columns)},
                )

                lower_cols = {c.lower(): c for c in df.columns}

                def pick_column(candidates):
                    for key in candidates:
                        if key in lower_cols:
                            return lower_cols[key]
                    return None

                symbol_col = pick_column(["stock_code", "stockcode", "exchangecode", "securitysymbol", "short_name"])
                lot_col = pick_column(["lot_size", "lotsize", "lot"])
                tick_col = pick_column(["tick_size", "ticksize"])
                scrip_code = pick_column(["shortname"]) # excluded scripcode
                instrument_col = pick_column(["instrument", "instrumentname"]) # excluded "token"

                expiry_col = pick_column(["expiry_date", "expirydate", "expiry"])
                strike_col = pick_column(["strike_price", "strikeprice", "strike"])
                option_col = pick_column(["option_type", "optiontype", "option"])

                if not symbol_col:
                    trading_logger.log_warning(
                        "Unable to infer symbol column from IciciDirect slice",
                        {"file": fname, "columns": list(df.columns)},
                    )
                    continue

                part = pd.DataFrame()
                # Breeze API methods expect stock_code (human-readable symbol), not token id.
                part["stock_code"] = df[symbol_col].astype(str).str.strip().str.upper()
                # Normalize index names in part: NIFTY 50 -> NIFTY, NIFTY BANK -> BANKNIFTY
                part["stock_code"] = part["stock_code"].replace("NIFTY 50", "NIFTY").replace("NIFTY BANK", "BANKNIFTY").replace("NIFTY MIDCAP","MIDCPNIFTY").replace("NIFTY FINANCIAL","FINNIFTY").replace("NIFTY NEXT 50", "NIFTYNXT50")

                # Assign exchange and segment based on file name
                if fname == "NSEScripMaster.txt":
                    part["Exch"] = "NSE"
                    part["ExchType"] = "CASH"
                elif fname == "BSEScripMaster.txt":
                    part["Exch"] = "BSE"
                    part["ExchType"] = "CASH"
                elif fname == "FONSEScripMaster.txt":
                    part["Exch"] = "NFO"
                    part["ExchType"] = "NFO"
                elif fname == "FOBSEScripMaster.txt":
                    part["Exch"] = "BFO"
                    part["ExchType"] = "BFO"
                else:
                    part["Exch"] = "UNKNOWN"
                    part["ExchType"] = "UNKNOWN"

                if lot_col and lot_col in df.columns:
                    part["LotSize"] = pd.to_numeric(df[lot_col], errors="coerce").fillna(1).astype(int)
                else:
                    part["LotSize"] = 1
                
                if fname in ["FONSEScripMaster.txt", "FOBSEScripMaster.txt"]:
                    if instrument_col and instrument_col in df.columns:
                        part["ExchType"] = df[instrument_col].astype(str).str.upper().str.strip().str[:3]
                    else:
                        part["ExchType"] = "DER"
                    
                    if expiry_col and expiry_col in df.columns:
                        part["ExpiryDate"] = df[expiry_col]
                    if strike_col and strike_col in df.columns:
                        part["StrikePrice"] = df[strike_col]
                    if option_col and option_col in df.columns:
                        part["OptionType"] = df[option_col]

                if tick_col and tick_col in df.columns:
                    part["TickSize"] = pd.to_numeric(df[tick_col], errors="coerce").fillna(0.05)
                    if fname in ["FONSEScripMaster.txt", "FOBSEScripMaster.txt"]:
                         part["TickSize"] = part["TickSize"] / 100.0
                else:
                    part["TickSize"] = 0.05

                def make_long_symbol(row):
                    sym = row["stock_code"]
                    sym_u = str(sym).upper().replace(" ", "")

                    # Cash: follow STK/IND pattern similar to other brokers
                    if row["ExchType"] == "CASH":
                        if any(idx in sym_u for idx in ["NIFTY", "BANKNIFTY", "SENSEX", "INDIA VIX", "INDIAVIX", "INDEX"]):
                            return f"{sym_u}_IND___"
                        else:
                            return f"{sym_u}_STK___"
                    # F&O: Construct SYMBOL_EXPIRY_STRIKE_TYPE
                    else:
                        try:
                            # Parse Expiry: 24-Dec-2025 -> 20251224
                            expiry_raw = str(row.get("ExpiryDate", ""))
                            if expiry_raw and expiry_raw != "nan":
                                try:
                                    dt_obj = dt.datetime.strptime(expiry_raw, "%d-%b-%Y")
                                    expiry_str = dt_obj.strftime("%Y%m%d")
                                except ValueError:
                                    expiry_str = expiry_raw
                            else:
                                expiry_str = ""

                            # Parse Strike: 31000.0 -> 31000
                            strike_raw = row.get("StrikePrice", 0)
                            try:
                                strike_val = float(strike_raw)
                                if strike_val.is_integer():
                                    strike_str = str(int(strike_val))
                                else:
                                    strike_str = str(strike_val)
                            except (ValueError, TypeError):
                                strike_str = str(strike_raw)

                            # Parse Option Type: PA->PUT, CA->CALL, XX->FUT
                            opt_raw = str(row.get("OptionType", "")).upper()
                            if opt_raw in ["PA", "PE"]:
                                opt_type = "PUT"
                            elif opt_raw in ["CA", "CE"]:
                                opt_type = "CALL"
                            elif opt_raw == "XX":
                                opt_type = "FUT"
                            else:
                                opt_type = opt_raw

                            if opt_type == "FUT":
                                return f"{sym_u}_FUT_{expiry_str}__"
                            else:
                                return f"{sym_u}_OPT_{expiry_str}_{opt_type}_{strike_str}"

                        except Exception:
                             return f"{sym_u}_DERIV___"
                
                part["long_symbol"] = part.apply(make_long_symbol, axis=1)
                if scrip_code and scrip_code in df.columns:
                    part["Scripcode"] = df[scrip_code].astype(str).str.strip()
                else:
                    # Fallback so downstream lookups still work even without explicit token column.
                    part["Scripcode"] = part["stock_code"]

                # Drop extra columns (e.g. ExpiryDate, StrikePrice, OptionType) after long_symbol is constructed.
                part = part[["stock_code", "Exch", "ExchType", "LotSize", "TickSize", "long_symbol", "Scripcode"]]
                parts.append(part)

        if not parts:
            raise DataError(
                "No recognizable IciciDirect symbol master slices found in ZIP",
                create_error_context(files=list(all_names)),
            )

        codes = pd.concat(parts, ignore_index=True)

        # Reorder columns to the common schema
        codes = codes[["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize", "stock_code"]]
        codes = codes.drop_duplicates(subset=["long_symbol", "Exch"], keep="first").reset_index(drop=True)

        if saveToFolder:
            dest_symbol_file = (
                f"{config.get('ICICIDIRECT.SYMBOLCODES')}/{dt.datetime.today().strftime('%Y%m%d')}_symbols.csv"
            )
            try:
                codes[["long_symbol", "LotSize", "Scripcode", "Exch", "ExchType", "TickSize"]].to_csv(
                    dest_symbol_file, index=False
                )
                trading_logger.log_info(
                    "IciciDirect symbols CSV written",
                    {"path": dest_symbol_file, "rows": len(codes)},
                )
            except Exception as e:
                trading_logger.log_error(
                    "Error writing IciciDirect symbols CSV", e, {"path": dest_symbol_file}
                )

        return codes

    except (ConfigurationError, DataError):
        raise
    except Exception as e:
        context = create_error_context(url=url, error=str(e))
        trading_logger.log_error("Error in IciciDirect save_symbol_data", e, {"url": url})
        raise DataError(f"Error fetching/parsing IciciDirect symbol data: {str(e)}", context)


def _format_expiry_for_breeze(expiry_yyyymmdd: str) -> str:
    """Convert YYYYMMDD to ISO8601 UTC for Breeze get_quotes (e.g. 20260326 -> 2026-03-26T06:00:00.000Z)."""
    if not expiry_yyyymmdd or len(expiry_yyyymmdd) != 8:
        return ""
    try:
        d = dt.datetime.strptime(expiry_yyyymmdd, "%Y%m%d")
        # Breeze expects expiry_date in ISO8601 UTC; use 06:00:00 UTC as expiry time.
        return d.strftime("%Y-%m-%d") + "T06:00:00.000Z"
    except ValueError:
        return expiry_yyyymmdd


# Timezone for Breeze API responses (exchange times are India)
ICICIDIRECT_TIMEZONE = "Asia/Kolkata"


def _parse_api_date_to_kolkata(
    ts: Union[str, float, int, None, dt.datetime, pd.Timestamp],
) -> pd.Timestamp:
    """
    Convert API date (string or number) to timezone-aware datetime in Asia/Kolkata.
    Breeze API returns dates as strings (e.g. ISO8601); we normalize to Asia/Kolkata
    so HistoricalData.date is always timezone-aware and consistent for downstream.
    """
    if ts is None:
        return pd.NaT  # type: ignore[return-value]
    dt_val = pd.to_datetime(ts)
    if pd.isna(dt_val):
        return dt_val
    tz = getattr(dt_val, "tz", None) or getattr(dt_val, "tzinfo", None)
    if tz is not None:
        return dt_val.tz_convert(ICICIDIRECT_TIMEZONE)
    return dt_val.tz_localize(ICICIDIRECT_TIMEZONE)


def my_handler(typ, value, trace):
    """
    Unhandled exception hook for this module, consistent with other brokers.
    """
    context = create_error_context(
        exception_type=typ.__name__,
        exception_value=str(value),
        traceback="".join(traceback.format_tb(trace)),
    )
    trading_logger.log_error(f"Uncaught exception: {typ.__name__}", value, context)


sys.excepthook = my_handler


class IciciDirect(BrokerBase):
    """
    ICICIDirect broker implementation using BreezeConnect SDK.

    This is a first-cut implementation that wires IciciDirect into the common
    BrokerBase interface. Several advanced methods are left as TODOs so they
    can be implemented incrementally.
    """

    @log_execution_time
    def __init__(self, **kwargs):
        """
        Initialize IciciDirect broker.

        Expected configuration keys (in config.py):
            - ICICIDIRECT.API_KEY
            - ICICIDIRECT.API_SECRET
            - ICICIDIRECT.API_SESSION_TOKEN  (or equivalent, depending on your auth flow)
        """
        try:
            super().__init__(**kwargs)
            self.broker = Brokers.ICICIDIRECT if hasattr(Brokers, "ICICIDIRECT") else Brokers.UNDEFINED
            self.api: BreezeConnect | None = None
            self.codes = pd.DataFrame()
            self.starting_order_ids_int: Dict[str, int] = {}
            self.redis_o = redis.Redis(db=0, encoding="utf-8", decode_responses=True)
            self.subscribed_symbols: List[str] = []
            self._stream_token_to_symbol: Dict[str, str] = {}
            self._stream_symbol_to_token: Dict[str, str] = {}
            self._stream_stock_code_to_symbol: Dict[str, str] = {}
            self._stream_symbol_meta: Dict[str, Dict[str, str]] = {}
            self._stream_external_callback = None
            self._stream_raw_tick_count = 0
            self._stream_mapped_tick_count = 0
            self._stream_last_tick_preview: Dict[str, object] = {}

            trading_logger.log_info(
                "IciciDirect broker initialized",
                {"broker_type": "IciciDirect", "config_keys": list(kwargs.keys())},
            )
        except Exception as e:
            context = create_error_context(
                broker_type="IciciDirect",
                config_keys=list(kwargs.keys()),
                error=str(e),
            )
            raise BrokerConnectionError(f"Failed to initialize IciciDirect broker: {str(e)}", context)

    # ------------------------------------------------------------------
    # Connection / session management
    # ------------------------------------------------------------------

    def _read_session_token_from_file(self, token_file_path: str, max_age_hours: int = 20) -> Optional[str]:
        """
        Read and validate a cached ICICIDirect session token from a file.

        The token is considered stale when the file is from a previous day, or when
        the file is older than max_age_hours (for same-day staleness).
        """
        if not token_file_path:
            return None

        if not os.path.exists(token_file_path):
            trading_logger.log_debug(
                "IciciDirect token file does not exist",
                {"token_file_path": token_file_path},
            )
            return None

        try:
            mod_time = os.path.getmtime(token_file_path)
            mod_datetime = dt.datetime.fromtimestamp(mod_time)
            today = dt.datetime.now().date()
            if mod_datetime.date() < today:
                trading_logger.log_info(
                    "IciciDirect cached token file is from a previous day, will regenerate",
                    {
                        "token_file_path": token_file_path,
                        "token_date": mod_datetime.date().isoformat(),
                        "today": today.isoformat(),
                    },
                )
                return None

            token_age_hours = (time.time() - mod_time) / 3600.0
            if token_age_hours > max_age_hours:
                trading_logger.log_info(
                    "IciciDirect cached token file is stale",
                    {
                        "token_file_path": token_file_path,
                        "token_age_hours": round(token_age_hours, 2),
                        "max_age_hours": max_age_hours,
                    },
                )
                return None

            with open(token_file_path, "r", encoding="utf-8") as f:
                token = f.read().strip()

            if not token:
                return None

            trading_logger.log_info(
                "IciciDirect session token loaded from cache file",
                {"token_file_path": token_file_path},
            )
            return token
        except Exception as e:
            trading_logger.log_warning(
                "Failed to read IciciDirect token cache file",
                {"token_file_path": token_file_path, "error": str(e)},
            )
            return None

    def _write_session_token_to_file(self, token_file_path: str, token: str) -> None:
        """Persist session token to disk for subsequent non-interactive logins."""
        if not token_file_path or not token:
            return

        try:
            os.makedirs(os.path.dirname(token_file_path), exist_ok=True) if os.path.dirname(token_file_path) else None
            with open(token_file_path, "w", encoding="utf-8") as f:
                f.write(token)
            trading_logger.log_info(
                "IciciDirect session token persisted to cache file",
                {"token_file_path": token_file_path},
            )
        except Exception as e:
            trading_logger.log_warning(
                "Failed to persist IciciDirect session token",
                {"token_file_path": token_file_path, "error": str(e)},
            )

    def _get_session_token_from_command(self, token_command: str) -> Optional[str]:
        """
        Execute a configured command to obtain a fresh session token.

        Expected behavior: command writes only the token to stdout.
        """
        if not token_command:
            return None

        # Expand environment variables even if users configured values with single quotes,
        # e.g. '--api-key '$ICICI_API_KEY'' in YAML.
        expanded_command = os.path.expandvars(token_command)
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        cmd_env = os.environ.copy()
        # Make common ICICI env names available to command scripts even if caller
        # did not export them in shell.
        cmd_env.update(
            {
                "ICICI_API_KEY": str(config.get("ICICIDIRECT.API_KEY") or ""),
                "ICICI_API_SECRET": str(config.get("ICICIDIRECT.API_SECRET") or ""),
                "ICICI_USER_ID": str(config.get("ICICIDIRECT.USER_ID") or config.get("ICICIDIRECT.USERNAME") or ""),
                "ICICI_PASSWORD": str(config.get("ICICIDIRECT.PASSWORD") or ""),
                "ICICI_TOTP_TOKEN": str(config.get("ICICIDIRECT.TOTP_TOKEN") or ""),
            }
        )

        try:
            result = subprocess.run(
                expanded_command,
                shell=True,
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
                cwd=repo_root,
                env=cmd_env,
            )
            token = (result.stdout or "").strip()
            if token:
                trading_logger.log_info(
                    "IciciDirect session token obtained via AUTO_SESSION_TOKEN_CMD",
                    {},
                )
                return token

            trading_logger.log_warning(
                "AUTO_SESSION_TOKEN_CMD executed but returned an empty token",
                {
                    "command": token_command,
                    "expanded_command": expanded_command,
                    "stdout": (result.stdout or "")[:500],
                    "stderr": (result.stderr or "")[:500],
                },
            )
            raise AuthenticationError(
                "AUTO_SESSION_TOKEN_CMD executed but returned an empty token",
                create_error_context(
                    command=token_command,
                    expanded_command=expanded_command,
                    stdout=(result.stdout or "")[:500],
                    stderr=(result.stderr or "")[:500],
                ),
            )
        except subprocess.CalledProcessError as e:
            stderr = (e.stderr or "").strip()
            stdout = (e.stdout or "").strip()
            trading_logger.log_warning(
                "AUTO_SESSION_TOKEN_CMD failed with non-zero exit",
                {
                    "command": token_command,
                    "expanded_command": expanded_command,
                    "returncode": e.returncode,
                    "stderr": stderr[:500],
                    "stdout": stdout[:500],
                },
            )
            raise AuthenticationError(
                f"AUTO_SESSION_TOKEN_CMD failed with return code {e.returncode}",
                create_error_context(
                    command=token_command,
                    expanded_command=expanded_command,
                    returncode=e.returncode,
                    stderr=stderr[:1000],
                    stdout=stdout[:1000],
                ),
            )
        except subprocess.TimeoutExpired as e:
            raise AuthenticationError(
                "AUTO_SESSION_TOKEN_CMD timed out",
                create_error_context(
                    command=token_command,
                    expanded_command=expanded_command,
                    timeout_seconds=60,
                    stdout=str(e.stdout),
                    stderr=str(e.stderr),
                ),
            )
        except Exception as e:
            trading_logger.log_warning(
                "AUTO_SESSION_TOKEN_CMD failed",
                {"command": token_command, "expanded_command": expanded_command, "error": str(e)},
            )
            raise AuthenticationError(
                f"AUTO_SESSION_TOKEN_CMD failed: {str(e)}",
                create_error_context(command=token_command, expanded_command=expanded_command, error=str(e)),
            )

    def _resolve_session_token(self) -> str:
        """
        Resolve a session token without user intervention.

        Resolution order:
        1) ICICIDIRECT.API_SESSION_TOKEN
        2) ICICIDIRECT.USERTOKEN (cached token file)
        3) ICICIDIRECT.AUTO_SESSION_TOKEN_CMD (external non-interactive token fetch)
        """
        configured_token = config.get("ICICIDIRECT.API_SESSION_TOKEN")
        if configured_token:
            return configured_token.strip()

        token_file_path = config.get("ICICIDIRECT.USERTOKEN")
        max_age_hours = int(config.get("ICICIDIRECT.USERTOKEN_MAX_AGE_HOURS") or 20)
        cached_token = self._read_session_token_from_file(token_file_path, max_age_hours=max_age_hours)
        if cached_token:
            return cached_token

        token_command = config.get("ICICIDIRECT.AUTO_SESSION_TOKEN_CMD")
        if not token_command and config.get("ICICIDIRECT.AUTO_LOGIN"):
            token_command = (
                "python -m tradingapi.icicidirect_generate_session "
                "--api-key \"${ICICI_API_KEY}\" "
                "--user-id \"${ICICI_USER_ID}\" "
                "--password \"${ICICI_PASSWORD}\" "
                "--totp-token \"${ICICI_TOTP_TOKEN}\""
            )
            # Optional selector / webdriver overrides from config.
            opt_map = {
                "LOGIN_USERNAME_SELECTOR": "--user-selectors",
                "LOGIN_PASSWORD_SELECTOR": "--password-selectors",
                "LOGIN_TNC_SELECTOR": "--tnc-selector",
                "LOGIN_SUBMIT_SELECTOR": "--submit-selectors",
                "TOTP_INPUT_SELECTOR": "--otp-selectors",
                "TOTP_SUBMIT_SELECTOR": "--otp-submit-selectors",
                "SELENIUM_WEBDRIVER_PATH": "--driver-path",
                "REDIRECT_TIMEOUT": "--redirect-wait",
            }
            for conf_key, arg_name in opt_map.items():
                v = config.get(f"ICICIDIRECT.{conf_key}")
                if v not in [None, ""]:
                    token_command += f" {arg_name} \"{v}\""

            if not bool(config.get("ICICIDIRECT.SELENIUM_HEADLESS", True)):
                token_command += " --no-headless"

        command_token = self._get_session_token_from_command(token_command)
        if command_token:
            self._write_session_token_to_file(token_file_path, command_token)
            return command_token

        raise ConfigurationError(
            "Unable to resolve ICICIDIRECT session token non-interactively. Configure one of: "
            "ICICIDIRECT.API_SESSION_TOKEN, ICICIDIRECT.USERTOKEN (cached file), "
            "or ICICIDIRECT.AUTO_SESSION_TOKEN_CMD.",
            create_error_context(
                configured_token_present=bool(configured_token),
                token_file_path=token_file_path,
                token_command_configured=bool(token_command),
            ),
        )

    @log_execution_time
    @retry_on_error(max_retries=3, delay=2.0, backoff_factor=2.0)
    def connect(
        self,
        redis_db: int,
        as_of_date: Optional[Union[dt.date, dt.datetime, str]] = None,
    ):
        """
        Initialize BreezeConnect session and internal Redis state.

        Args:
            redis_db: Redis database number
            as_of_date: Optional date (date, datetime, or str YYYYMMDD/YYYY-MM-DD).
                If provided, TRADINGAPI_NOW is set so the broker behaves as if on that date.
        """
        normalized = _normalize_as_of_date(as_of_date)
        if normalized is not None:
            tradingapi_globals.TRADINGAPI_NOW = normalized

        previous_proxy_env = None
        try:
            from .proxy_utils import set_proxy_env_for_broker, restore_proxy_env
            previous_proxy_env = set_proxy_env_for_broker(self.broker.name)
        except Exception:
            pass

        _prev_ipv6 = _temporarily_force_ipv4()
        try:
            api_key = config.get("ICICIDIRECT.API_KEY")
            api_secret = config.get("ICICIDIRECT.API_SECRET")
            session_token = self._resolve_session_token()

            if not api_key or not api_secret:
                raise ConfigurationError(
                    "Missing ICICIDIRECT credentials in config",
                    create_error_context(
                        api_key_present=bool(api_key),
                        api_secret_present=bool(api_secret),
                    ),
                )

            self.api = BreezeConnect(api_key=api_key)
            self.api.generate_session(api_secret=api_secret, session_token=session_token)

            # Validate session immediately and persist latest token in cache file when configured.
            customer_details = self.api.get_customer_details(api_session=session_token)
            if not isinstance(customer_details, dict):
                raise AuthenticationError(
                    "Unexpected response validating ICICIDirect session",
                    create_error_context(response_type=str(type(customer_details))),
                )

            if customer_details.get("Error"):
                raise AuthenticationError(
                    f"ICICIDirect authentication failed: {customer_details.get('Error')}",
                    create_error_context(response=customer_details),
                )

            token_file_path = config.get("ICICIDIRECT.USERTOKEN")
            self._write_session_token_to_file(token_file_path, session_token)

            self.redis_o = redis.Redis(db=redis_db, encoding="utf-8", decode_responses=True)
            self.starting_order_ids_int = set_starting_internal_ids_int(self.redis_o)

            # Load or generate symbol file (same pattern as FivePaisa)
            try:
                self.codes = self.update_symbology()
                trading_logger.log_debug(
                    "IciciDirect symbology updated",
                    {"codes_shape": self.codes.shape if hasattr(self.codes, "shape") else None},
                )
            except Exception as e:
                trading_logger.log_warning("Failed to update IciciDirect symbology", {"error": str(e)})

            trading_logger.log_info(
                "IciciDirect connected",
                {"redis_db": redis_db},
            )
            return True
        except (AuthenticationError, ConfigurationError):
            raise
        except Exception as e:
            context = create_error_context(
                broker="IciciDirect",
                error=str(e),
            )
            raise BrokerConnectionError(f"Error connecting to IciciDirect: {str(e)}", context)
        finally:
            _restore_ipv6(_prev_ipv6)
            try:
                from .proxy_utils import restore_proxy_env
                restore_proxy_env(previous_proxy_env)
            except Exception:
                pass

    def is_connected(self):
        """
        Lightweight connectivity check.
        """
        return self.api is not None

    def disconnect(self):
        """
        BreezeConnect is HTTP-based and generally stateless; we just drop the client.
        """
        try:
            try:
                self.stop_streaming()
            except Exception:
                pass
            self.api = None
            trading_logger.log_info("IciciDirect disconnected", {})
        except Exception as e:
            context = create_error_context(error=str(e))
            raise BrokerConnectionError(f"Failed to disconnect IciciDirect: {str(e)}", context)

    # ------------------------------------------------------------------
    # Symbology / exchange mapping
    # ------------------------------------------------------------------

    def update_symbology(self, **kwargs):
        """
        Load or generate IciciDirect symbol master and build exchange_mappings.
        If ICICIDIRECT.SYMBOLCODES is set and a symbols file exists for the current
        trading date (get_tradingapi_now()), load from CSV; otherwise download and
        generate via save_symbol_data() and optionally save to folder.
        """
        try:
            save_to_folder = kwargs.get("saveToFolder", True)
            symbol_codes_path = config.get("ICICIDIRECT.SYMBOLCODES")
            date_str = get_tradingapi_now().strftime("%Y%m%d")

            if symbol_codes_path:
                symbols_path = os.path.join(symbol_codes_path, f"{date_str}_symbols.csv")
                if os.path.exists(symbols_path):
                    trading_logger.log_info(
                        "Loading existing IciciDirect symbols file",
                        {"symbols_path": symbols_path, "date": date_str},
                    )
                    codes = pd.read_csv(symbols_path)
                    self.codes = codes
                else:
                    trading_logger.log_info(
                        "IciciDirect symbols file not found, generating",
                        {"symbols_path": symbols_path},
                    )
                    codes = save_symbol_data(saveToFolder=save_to_folder)
                    self.codes = codes
            else:
                codes = save_symbol_data(saveToFolder=False)
                self.codes = codes

            # Build exchange_mappings in the same structure used by other brokers.
            # Use Scripcode so mappings work both when loading from CSV (which has no stock_code) and from save_symbol_data().
            self.exchange_mappings = {}
            scrip_col = "Scripcode" if "Scripcode" in codes.columns else "stock_code"
            for exchange, group in codes.groupby("Exch"):
                try:
                    self.exchange_mappings[exchange] = {
                        # Breeze APIs use this as stock_code / instrument id.
                        "symbol_map": dict(zip(group["long_symbol"], group[scrip_col])),
                        "contractsize_map": dict(zip(group["long_symbol"], group["LotSize"])),
                        "exchange_map": dict(zip(group["long_symbol"], group["Exch"])),
                        "exchangetype_map": dict(zip(group["long_symbol"], group["ExchType"])),
                        "contracttick_map": dict(zip(group["long_symbol"], group["TickSize"])),
                        "symbol_map_reversed": dict(zip(group[scrip_col], group["long_symbol"])),
                        "brokerid_map_reversed": dict(zip(group[scrip_col], group["long_symbol"])),
                    }

                    trading_logger.log_debug(
                        "IciciDirect exchange mappings created",
                        {"exchange": exchange, "symbol_count": len(group)},
                    )
                except Exception as e:
                    trading_logger.log_error(
                        "Error creating IciciDirect mappings for exchange",
                        e,
                        {"exchange": exchange, "group_shape": group.shape},
                    )
                    continue

            trading_logger.log_info(
                "IciciDirect symbology update completed",
                {"total_exchanges": len(self.exchange_mappings), "total_symbols": len(codes)},
            )

            return codes
        except (ConfigurationError, DataError):
            raise
        except Exception as e:
            context = create_error_context(error=str(e))
            raise DataError(f"Unexpected error updating IciciDirect symbology: {str(e)}", context)

    def map_exchange_for_api(self, long_symbol, exchange) -> str:
        """
        Map internal exchange representation to Breeze exchange_code.
        """
        ex = str(exchange or "").upper().strip()
        sym = str(long_symbol or "").upper()

        # Derivative symbols are quoted/traded via FO exchanges.
        is_derivative = "_FUT_" in sym or "_OPT_" in sym
        if is_derivative:
            if ex in ["N", "NSE", "NFO"]:
                return "NFO"
            if ex in ["B", "BSE", "BFO"]:
                return "BFO"

        mapping = {
            "N": "NSE",
            "NSE": "NSE",
            "B": "BSE",
            "BSE": "BSE",
            "NFO": "NFO",
            "BFO": "BFO",
        }
        return mapping.get(ex, ex)

    def map_exchange_for_db(self, long_symbol, exchange) -> str:
        """
        Map exchange for database usage. Currently same as API mapping.
        """
        return self.map_exchange_for_api(long_symbol, exchange)

    def _call_api_method(self, method_name: str, payload: Dict[str, object]) -> dict:
        """
        Call a BreezeConnect method with keyword filtering based on runtime signature.
        This avoids TypeError from unsupported kwargs across breeze_connect versions.
        Also forces IPv4 for the request window because Breeze endpoints can intermittently
        fail over IPv6 with non-JSON responses.
        """
        if self.api is None:
            raise BrokerConnectionError("IciciDirect not connected", create_error_context())

        method = getattr(self.api, method_name)
        sig = inspect.signature(method)
        accepts_kwargs = any(param.kind == inspect.Parameter.VAR_KEYWORD for param in sig.parameters.values())
        if accepts_kwargs:
            filtered_payload = payload
        else:
            filtered_payload = {k: v for k, v in payload.items() if k in sig.parameters}

        _prev_ipv6 = _temporarily_force_ipv4()
        try:
            return method(**filtered_payload)
        finally:
            _restore_ipv6(_prev_ipv6)

    def _today_window_utc(self) -> tuple[str, str]:
        now = get_tradingapi_now()
        day = now.strftime("%Y-%m-%d")
        return f"{day}T00:00:00.000Z", f"{day}T23:59:59.000Z"

    def _resolve_exchange_code_for_order(self, broker_order_id: str = "", fallback: str = "NSE") -> str:
        """
        Resolve exchange_code for order APIs using kwargs/redis context, defaulting safely.
        """
        if broker_order_id and hasattr(self, "redis_o"):
            try:
                order_data = self.redis_o.hgetall(str(broker_order_id))
                long_symbol = str(order_data.get("long_symbol") or "").strip()
                exchange = str(order_data.get("exchange") or "").strip()
                if long_symbol:
                    return self.map_exchange_for_api(long_symbol, exchange or fallback)
            except Exception:
                pass
        return str(fallback or "NSE").upper()

    def _sanitize_user_remark(self, value: str) -> str:
        """
        Breeze place_order enforces alphanumeric-only user_remark.
        """
        raw = str(value or "")
        cleaned = "".join(ch for ch in raw if ch.isalnum())
        if cleaned:
            return cleaned[:20]
        return "ORD" + get_tradingapi_now().strftime("%H%M%S")

    def _persist_order_links(self, order: Order) -> None:
        """
        Persist order object and link broker order id into internal order's entry/exit keys.
        Mirrors behavior expected by shared utils for direct broker.place_order() calls.
        """
        broker_order_id = str(order.broker_order_id or "").strip()
        internal_order_id = str(order.internal_order_id or "").strip()
        order_ref = str(order.orderRef or internal_order_id).strip()
        if not broker_order_id or not internal_order_id:
            return

        existing = self.redis_o.hgetall(broker_order_id) if hasattr(self, "redis_o") else {}
        if not existing:
            self.redis_o.hmset(broker_order_id, {key: str(val) for key, val in order.to_dict().items()})

        if order.order_type in ["BUY", "SHORT"]:
            current_keys = self.redis_o.hget(internal_order_id, "entry_keys") or ""
            key_list = [k for k in str(current_keys).split() if k]
            if broker_order_id not in key_list:
                key_list.append(broker_order_id)
                self.redis_o.hset(order_ref, "entry_keys", " ".join(key_list))
                self.redis_o.hset(internal_order_id, "long_symbol", str(order.long_symbol))
        elif order.order_type in ["SELL", "COVER"]:
            current_keys = self.redis_o.hget(internal_order_id, "exit_keys") or ""
            key_list = [k for k in str(current_keys).split() if k]
            if broker_order_id not in key_list:
                key_list.append(broker_order_id)
                self.redis_o.hset(order_ref, "exit_keys", " ".join(key_list))
                self.redis_o.hset(internal_order_id, "long_symbol", str(order.long_symbol))

    def _normalize_order_status(
        self,
        status_raw: str,
        fill_size: int,
        order_size: int,
        exchange_order_id: str = "",
    ) -> OrderStatus:
        """
        Normalize broker-native status strings to common OrderStatus values.
        """
        s = str(status_raw or "").strip().upper()
        exch_order_id = str(exchange_order_id or "").strip()

        if any(tok in s for tok in ["REJECT", "FAIL", "DENIED", "ERROR"]):
            return OrderStatus.REJECTED
        if any(tok in s for tok in ["CANCEL", "CANCELLED", "CANCELED", "EXPIRE"]):
            return OrderStatus.CANCELLED
        if any(tok in s for tok in ["COMPLETE", "COMPLETED", "EXECUTED", "TRADED", "FILLED"]):
            return OrderStatus.FILLED
        if any(tok in s for tok in ["PENDING", "TRIGGER", "WAIT", "RECEIVED", "VALIDATION"]):
            return OrderStatus.PENDING
        if any(tok in s for tok in ["OPEN", "WORKING", "PARTIAL", "MODIFIED", "ORDERED"]):
            return OrderStatus.OPEN

        # Quantity/exchange-id fallbacks to keep behavior consistent with other brokers.
        if order_size > 0 and fill_size >= order_size:
            return OrderStatus.FILLED
        if fill_size > 0:
            return OrderStatus.OPEN
        if exch_order_id and exch_order_id not in ["0", "None", "NONE"]:
            return OrderStatus.OPEN
        if s:
            return OrderStatus.PENDING
        return OrderStatus.UNDEFINED

    def _resolve_stock_code_for_symbol(self, long_symbol: str, mapped_exchange: str) -> str:
        """Resolve stock_code from long_symbol with tolerant fallbacks for derivatives/index options."""
        if mapped_exchange not in self.exchange_mappings:
            raise SymbolError(
                f"Exchange {mapped_exchange} not available for IciciDirect",
                create_error_context(mapped_exchange=mapped_exchange, available_exchanges=list(self.exchange_mappings.keys())),
            )

        symbol_map = self.exchange_mappings[mapped_exchange].get("symbol_map", {})
        stock_code = symbol_map.get(long_symbol)
        if stock_code:
            return stock_code

        # Fallback: try matching by instrument root symbol prefix (e.g. INFY_* / NIFTY_* / SENSEX_*).
        base_symbol = str(long_symbol).split("_")[0].upper()
        for lsym, scode in symbol_map.items():
            if str(lsym).upper().startswith(base_symbol + "_"):
                trading_logger.log_warning(
                    "IciciDirect symbol exact match not found; using fallback base-symbol match",
                    {"requested_symbol": long_symbol, "matched_symbol": lsym, "exchange": mapped_exchange},
                )
                return str(scode)

        raise SymbolError(
            f"Symbol {long_symbol} not found for exchange {mapped_exchange} in IciciDirect mappings",
            create_error_context(long_symbol=long_symbol, mapped_exchange=mapped_exchange),
        )

    def _get_quotes_params_from_long_symbol(
        self, long_symbol: str, exchange: str
    ) -> Dict[str, str]:
        """
        Explode long_symbol into Breeze get_quotes(...) parameters.

        long_symbol formats: SYMBOL_STK___, SYMBOL_IND___, SYMBOL_FUT_YYYYMMDD__,
        SYMBOL_OPT_YYYYMMDD_CALL_19500 / SYMBOL_OPT_YYYYMMDD_PUT_19500.
        Returns dict with keys: stock_code, exchange_code, expiry_date, product_type, right, strike_price.
        """
        mapped_exchange = self.map_exchange_for_api(long_symbol, exchange)
        stock_code = self._resolve_stock_code_for_symbol(long_symbol, mapped_exchange)
        lsym = str(long_symbol).strip().upper()
        parts = lsym.split("_")

        out = {
            "stock_code": str(stock_code),
            "exchange_code": mapped_exchange,
            "expiry_date": "",
            "product_type": "cash",
            "right": "",
            "strike_price": "",
        }

        if "_OPT_" in lsym and len(parts) >= 5:
            # SYMBOL_OPT_YYYYMMDD_CALL_19500 or _PUT_19500
            out["product_type"] = "options"
            out["expiry_date"] = _format_expiry_for_breeze(parts[2])
            out["right"] = "call" if parts[3] == "CALL" else "put"
            out["strike_price"] = parts[4] if len(parts) > 4 else ""
        elif "_FUT_" in lsym and len(parts) >= 3:
            # SYMBOL_FUT_YYYYMMDD__; Breeze expects right="others", strike_price="0" for futures.
            out["product_type"] = "futures"
            out["expiry_date"] = _format_expiry_for_breeze(parts[2])
            out["right"] = "others"
            out["strike_price"] = "0"

        return out

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    @log_execution_time
    @validate_inputs(long_symbol=lambda s: isinstance(s, str) and len(s.strip()) > 0)
    def get_quote(self, long_symbol: str, exchange="NSE") -> Price:
        """
        Get a quote from BreezeConnect.
        Explodes long_symbol (including F&O) into stock_code, exchange_code,
        expiry_date, product_type, right, strike_price per Breeze get_quotes() signature.
        """
        if self.api is None:
            raise BrokerConnectionError("IciciDirect not connected", create_error_context())

        market_feed = Price()
        market_feed.symbol = long_symbol
        market_feed.exchange = exchange
        market_feed.src = "ICICIDIRECT"
        market_feed.timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            params = self._get_quotes_params_from_long_symbol(long_symbol, exchange)

            quote_payload = {
                "stock_code": params["stock_code"],
                "exchange_code": params["exchange_code"],
                "expiry_date": params["expiry_date"],
                "product_type": params["product_type"],
                "right": params["right"],
                "strike_price": str(params["strike_price"]),
            }
            last_quote_error: Exception | None = None
            resp: object = {}
            for attempt in range(2):
                try:
                    resp = self._call_api_method("get_quotes", quote_payload)
                    break
                except Exception as quote_error:
                    last_quote_error = quote_error
                    if attempt == 0:
                        time.sleep(0.25)
                    else:
                        raise last_quote_error

            success = resp.get("Success") if isinstance(resp, dict) else None
            if isinstance(success, list):
                md = success[0] if success else None
            else:
                md = success if success is not None else None
            if md is None:
                trading_logger.log_warning(
                    "IciciDirect returned no quote data for this symbol",
                    {"long_symbol": long_symbol, "exchange": exchange, "response": resp},
                )
                return market_feed

            def _float(v, default=float("nan")):
                if v is None or v == "":
                    return default
                try:
                    return float(v)
                except (ValueError, TypeError):
                    return default

            def _int(v, default=0):
                if v is None or v == "":
                    return default
                try:
                    return int(float(v))
                except (ValueError, TypeError):
                    return default

            bid = _float(md.get("best_bid_price"))
            ask = _float(md.get("best_offer_price") or md.get("best_ask_price"))
            last = _float(md.get("ltp"))
            high = _float(md.get("high"))
            low = _float(md.get("low"))
            volume = _int(md.get("total_quantity_traded") or md.get("volume"))
            bid_vol = _int(md.get("best_bid_quantity"))
            ask_vol = _int(md.get("best_offer_quantity") or md.get("best_ask_quantity"))
            prior_close = _float(md.get("previous_close"))

            ltt = md.get("ltt") or ""
            if ltt:
                try:
                    timestamp = format_datetime(ltt, "%Y-%m-%d %H:%M:%S")  # type: ignore[name-defined]
                except (ValueError, TypeError):
                    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            return Price(
                bid=bid,
                ask=ask,
                bid_volume=bid_vol,
                ask_volume=ask_vol,
                prior_close=prior_close,
                last=last,
                high=high,
                low=low,
                volume=volume,
                symbol=long_symbol,
                exchange=exchange,
                src="ICICIDIRECT",
                timestamp=timestamp,
            )
        except Exception as e:
            trading_logger.log_error(
                "Error getting quote from IciciDirect",
                e,
                {"long_symbol": long_symbol, "exchange": exchange, "error": str(e)},
            )
            return market_feed

    def _stream_exchange_prefix(self, exchange_code: str) -> str:
        ex = str(exchange_code or "").upper()
        mapping = {
            "NSE": "4",
            "NFO": "4",
            "BSE": "1",
            "BFO": "8",
            "NDX": "13",
            "MCX": "6",
        }
        return mapping.get(ex, "4")

    def _build_stream_token(self, long_symbol: str, exchange: str) -> tuple[str, str]:
        mapped_exchange = self.map_exchange_for_api(long_symbol, exchange)
        scrip_code = self._resolve_stock_code_for_symbol(long_symbol, mapped_exchange)
        prefix = self._stream_exchange_prefix(mapped_exchange)
        # 1! = exchange quote stream token in Breeze
        token = f"{prefix}.1!{scrip_code}"
        return mapped_exchange, token

    def _resolve_long_symbol_from_tick(self, token: str, tick: Dict[str, object]) -> Optional[str]:
        def _norm(s: str) -> str:
            return "".join(ch for ch in str(s or "").upper() if ch.isalnum())

        # 1) Direct token mapping (preferred)
        if token in self._stream_token_to_symbol:
            return self._stream_token_to_symbol[token]

        # 2) Resolve token id via symbol_map_reversed in current exchange mappings
        try:
            token_id = token.split("!", 1)[1] if "!" in token else ""
            if token_id:
                for exch in self.exchange_mappings.values():
                    rev = exch.get("symbol_map_reversed", {})
                    if token_id in rev:
                        return str(rev[token_id])
                    try:
                        token_id_int = int(token_id)
                        if token_id_int in rev:
                            return str(rev[token_id_int])
                    except Exception:
                        pass
        except Exception:
            pass

        # 3) Fallback on stock_name populated by Breeze on_ticks enrichment.
        stock_name = str(tick.get("stock_name") or "").strip().upper().replace(" ", "")
        if stock_name:
            cands = [s for s in self.subscribed_symbols if str(s).upper().startswith(stock_name + "_")]
            if len(cands) == 1:
                return cands[0]

        # 4) Fallback on stock_code present in quote payload.
        stock_code = str(tick.get("stock_code") or "").strip().upper().replace(" ", "")
        if stock_code:
            if stock_code in self._stream_stock_code_to_symbol:
                return self._stream_stock_code_to_symbol[stock_code]
            cands = [s for s in self.subscribed_symbols if str(s).upper().startswith(stock_code + "_")]
            if len(cands) == 1:
                return cands[0]

        # 5) Decode token via Breeze helper and map by stock_name/root symbol.
        try:
            if self.api is not None and token:
                token_data = self._call_api_method("get_data_from_stock_token_value", {"input_stock_token": token})
                token_stock_name = str(token_data.get("stock_name") or "").strip()
                if token_stock_name:
                    token_key = _norm(token_stock_name)

                    # Prefer metadata-based exact matching on subscribed symbols.
                    cands = []
                    for s in self.subscribed_symbols:
                        meta = self._stream_symbol_meta.get(s, {})
                        meta_stock_code = _norm(meta.get("stock_code", ""))
                        sym_root = _norm(str(s).split("_", 1)[0])
                        if token_key and (token_key == meta_stock_code or token_key == sym_root):
                            cands.append(s)

                    # Fallback: token key may be long company name; match by inclusion with root/code.
                    if len(cands) == 0 and token_key:
                        for s in self.subscribed_symbols:
                            meta = self._stream_symbol_meta.get(s, {})
                            meta_stock_code = _norm(meta.get("stock_code", ""))
                            sym_root = _norm(str(s).split("_", 1)[0])
                            if (
                                (meta_stock_code and (meta_stock_code in token_key or token_key in meta_stock_code))
                                or (sym_root and (sym_root in token_key or token_key in sym_root))
                            ):
                                cands.append(s)

                    if len(cands) > 1:
                        product_type = str(token_data.get("product_type") or "").strip().lower()
                        right = str(token_data.get("right") or "").strip().lower()
                        strike = str(token_data.get("strike_price") or "").strip()
                        expiry = str(token_data.get("expiry_date") or "").strip()
                        try:
                            expiry_yyyymmdd = pd.to_datetime(expiry, errors="coerce").strftime("%Y%m%d") if expiry else ""
                        except Exception:
                            expiry_yyyymmdd = ""

                        if product_type == "options":
                            cands = [s for s in cands if "_OPT_" in s]
                            if right in ("call", "put"):
                                suffix = "_CALL_" if right == "call" else "_PUT_"
                                cands = [s for s in cands if suffix in s]
                            if strike:
                                cands = [s for s in cands if s.endswith(f"_{strike}") or f"_{strike}_" in s]
                            if expiry_yyyymmdd:
                                cands = [s for s in cands if f"_OPT_{expiry_yyyymmdd}_" in s]
                        elif product_type == "futures":
                            cands = [s for s in cands if "_FUT_" in s]
                            if expiry_yyyymmdd:
                                cands = [s for s in cands if f"_FUT_{expiry_yyyymmdd}" in s]
                        else:
                            # cash/index symbols
                            cands = [s for s in cands if ("_STK_" in s or "_IND_" in s)]

                    if len(cands) == 1:
                        self._stream_token_to_symbol[token] = cands[0]
                        return cands[0]
        except Exception:
            pass
        return None

    def _map_stream_tick_to_price(self, tick: Dict[str, object]) -> Optional[Price]:
        token = str(tick.get("symbol") or "").strip()
        long_symbol = self._resolve_long_symbol_from_tick(token, tick)
        if not long_symbol:
            return None

        market_feed = Price()
        market_feed.src = "ICICIDIRECT"
        market_feed.symbol = long_symbol
        market_feed.exchange = str(tick.get("exchange") or "")
        market_feed.timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        def _f(k: str, default=float("nan")):
            v = tick.get(k)
            if v in [None, ""]:
                return default
            try:
                return float(v)
            except Exception:
                return default

        def _i(k: str, default=0):
            v = tick.get(k)
            if v in [None, ""]:
                return default
            try:
                return int(float(v))
            except Exception:
                return default

        market_feed.bid = _f("bPrice")
        market_feed.ask = _f("sPrice")
        market_feed.bid_volume = _i("bQty")
        market_feed.ask_volume = _i("sQty")
        market_feed.last = _f("last")
        market_feed.high = _f("high")
        market_feed.low = _f("low")
        market_feed.prior_close = _f("close")
        market_feed.volume = _i("ttq")
        if isinstance(tick.get("ltt"), str) and tick.get("ltt"):
            market_feed.timestamp = str(tick.get("ltt"))
        return market_feed

    @log_execution_time
    @validate_inputs(
        operation=lambda x: isinstance(x, str) and x in ["s", "u"],
        symbols=lambda x: isinstance(x, list) and len(x) > 0,
        exchange=lambda x: isinstance(x, str) and len(x.strip()) > 0,
    )
    def start_quotes_streaming(self, operation: str, symbols: List[str], ext_callback=None, exchange="NSE") -> None:
        if self.api is None:
            raise BrokerConnectionError("IciciDirect not connected", create_error_context())

        try:
            if not self.exchange_mappings:
                self.update_symbology(saveToFolder=False)

            if ext_callback is not None:
                self._stream_external_callback = ext_callback

            def _on_tick(tick):
                try:
                    if not isinstance(tick, dict):
                        return
                    self._stream_raw_tick_count += 1
                    # Keep only a small safe preview for debug visibility.
                    self._stream_last_tick_preview = {k: tick.get(k) for k in list(tick.keys())[:12]}
                    price = self._map_stream_tick_to_price(tick)
                    if price is not None:
                        self._stream_mapped_tick_count += 1
                        if callable(self._stream_external_callback):
                            self._stream_external_callback(price)
                except Exception as callback_error:
                    trading_logger.log_error(
                        "Error in IciciDirect stream callback",
                        callback_error,
                        {"tick_preview": str(tick)[:500]},
                    )

            # Set callback before connection so first tick is not dropped.
            self.api.on_ticks = _on_tick

            if operation == "s":
                self._stream_raw_tick_count = 0
                self._stream_mapped_tick_count = 0
                self._stream_last_tick_preview = {}
                self._call_api_method("ws_connect", {})

            for raw_symbol in symbols:
                symbol = str(raw_symbol).split("?", 1)[0].strip()
                params = self._get_quotes_params_from_long_symbol(symbol, exchange)
                mapped_exchange = str(params.get("exchange_code") or self.map_exchange_for_api(symbol, exchange))
                stock_code = str(params.get("stock_code") or "").strip()
                if not stock_code:
                    raise MarketDataError(
                        "Unable to resolve stock_code for streaming subscription",
                        create_error_context(symbol=symbol, exchange=exchange),
                    )

                if operation == "s":
                    # Resolve Breeze websocket token for this instrument and cache mapping.
                    try:
                        exchange_token, _ = self._call_api_method(
                            "get_stock_token_value",
                            {
                                "exchange_code": mapped_exchange,
                                "stock_code": stock_code,
                                "product_type": str(params.get("product_type") or "cash"),
                                "expiry_date": str(params.get("expiry_date") or ""),
                                "strike_price": str(params.get("strike_price") or ""),
                                "right": str(params.get("right") or ""),
                                "get_exchange_quotes": True,
                                "get_market_depth": False,
                            },
                        )
                        if exchange_token:
                            self._stream_token_to_symbol[str(exchange_token)] = symbol
                    except Exception:
                        # Non-fatal: stream can still run; callback resolver has other fallbacks.
                        pass

                    sub_resp = self._call_api_method(
                        "subscribe_feeds",
                        {
                            "exchange_code": mapped_exchange,
                            "stock_code": stock_code,
                            "get_exchange_quotes": True,
                            "get_market_depth": False,
                        },
                    )
                    if isinstance(sub_resp, str) and "Exception while subscribing to feeds" in sub_resp:
                        raise MarketDataError(
                            f"IciciDirect subscribe failed: {sub_resp}",
                            create_error_context(symbol=symbol, exchange=mapped_exchange, stock_code=stock_code),
                        )
                    self._stream_stock_code_to_symbol[stock_code.upper()] = symbol
                    self._stream_symbol_to_token[symbol] = stock_code
                    self._stream_symbol_meta[symbol] = {
                        "exchange_code": mapped_exchange,
                        "stock_code": stock_code,
                        "product_type": str(params.get("product_type") or "cash"),
                        "expiry_date": str(params.get("expiry_date") or ""),
                        "right": str(params.get("right") or ""),
                        "strike_price": str(params.get("strike_price") or ""),
                    }
                    if symbol not in self.subscribed_symbols:
                        self.subscribed_symbols.append(symbol)
                    trading_logger.log_info(
                        "IciciDirect subscribed to market stream",
                        {"symbol": symbol, "exchange": mapped_exchange, "stock_code": stock_code},
                    )
                else:
                    unsub_stock_code = self._stream_symbol_to_token.get(symbol, stock_code)
                    # Best-effort token lookup for cleanup map
                    try:
                        exchange_token, _ = self._call_api_method(
                            "get_stock_token_value",
                            {
                                "exchange_code": mapped_exchange,
                                "stock_code": str(unsub_stock_code),
                                "product_type": str(params.get("product_type") or "cash"),
                                "expiry_date": str(params.get("expiry_date") or ""),
                                "strike_price": str(params.get("strike_price") or ""),
                                "right": str(params.get("right") or ""),
                                "get_exchange_quotes": True,
                                "get_market_depth": False,
                            },
                        )
                        if exchange_token:
                            self._stream_token_to_symbol.pop(str(exchange_token), None)
                    except Exception:
                        pass
                    unsub_resp = self._call_api_method(
                        "unsubscribe_feeds",
                        {
                            "exchange_code": mapped_exchange,
                            "stock_code": unsub_stock_code,
                            "get_exchange_quotes": True,
                            "get_market_depth": False,
                        },
                    )
                    if isinstance(unsub_resp, str) and "Exception while unsubscribing to feeds" in unsub_resp:
                        raise MarketDataError(
                            f"IciciDirect unsubscribe failed: {unsub_resp}",
                            create_error_context(symbol=symbol, stock_code=unsub_stock_code),
                        )
                    self._stream_stock_code_to_symbol.pop(str(unsub_stock_code).upper(), None)
                    self._stream_symbol_to_token.pop(symbol, None)
                    self._stream_symbol_meta.pop(symbol, None)
                    self.subscribed_symbols = [s for s in self.subscribed_symbols if s != symbol]
                    trading_logger.log_info(
                        "IciciDirect unsubscribed from market stream",
                        {"symbol": symbol, "stock_code": unsub_stock_code},
                    )
        except Exception as e:
            raise MarketDataError(
                f"Error starting quotes streaming for IciciDirect: {str(e)}",
                create_error_context(operation=operation, symbols=symbols, exchange=exchange, error=str(e)),
            )

    @log_execution_time
    def stop_streaming(self) -> None:
        if self.api is None:
            return
        try:
            for token in list(self._stream_token_to_symbol.keys()):
                try:
                    self._call_api_method(
                        "unsubscribe_feeds",
                        {
                            "stock_token": token,
                            "get_exchange_quotes": True,
                            "get_market_depth": False,
                        },
                    )
                except Exception:
                    pass
            self._stream_token_to_symbol = {}
            self._stream_symbol_to_token = {}
            self._stream_stock_code_to_symbol = {}
            self._stream_symbol_meta = {}
            self.subscribed_symbols = []
            self._call_api_method("ws_disconnect", {})
        except Exception as e:
            raise BrokerConnectionError(
                f"Error stopping IciciDirect streaming: {str(e)}",
                create_error_context(error=str(e)),
            )

    @log_execution_time
    def get_historical(
        self,
        symbols: Union[str, pd.DataFrame, dict],
        date_start: Union[str, dt.datetime, dt.date],
        date_end: Union[str, dt.datetime, dt.date] = get_tradingapi_now().strftime("%Y-%m-%d"),
        exchange: str = "N",
        periodicity: str = "1m",
        market_close_time: str = "15:30:00",
        refresh_mapping: bool = False,
    ) -> Dict[str, List[HistoricalData]]:
        """
        Get historical OHLCV data using Breeze historical API.

        This method currently supports a single symbol input (string).
        """
        if self.api is None:
            raise BrokerConnectionError("IciciDirect not connected", create_error_context())

        try:
            symbol = symbols if isinstance(symbols, str) else None
            if not symbol:
                raise ValidationError(
                    "IciciDirect.get_historical currently supports a single symbol string",
                    create_error_context(symbols_type=str(type(symbols))),
                )

            if refresh_mapping or not self.exchange_mappings:
                self.update_symbology(saveToFolder=False)

            mapped_exchange = self.map_exchange_for_api(symbol, exchange)
            stock_code = self.exchange_mappings.get(mapped_exchange, {}).get("symbol_map", {}).get(symbol)
            if not stock_code:
                raise SymbolError(
                    f"Symbol {symbol} not found for exchange {mapped_exchange}",
                    create_error_context(symbol=symbol, exchange=mapped_exchange),
                )

            from_date = pd.to_datetime(date_start).strftime("%Y-%m-%dT00:00:00.000Z")
            to_date = pd.to_datetime(date_end).strftime("%Y-%m-%dT23:59:59.000Z")

            interval = {
                "1m": "1minute",
                "5m": "5minute",
                "15m": "15minute",
                "30m": "30minute",
                "1h": "1hour",
                "1d": "1day",
                "D": "1day",
            }.get(periodicity, periodicity)

            # For NFO/BFO, API requires expiry_date and product_type (options/futures), right, strike_price.
            kwargs: Dict[str, str] = {
                "interval": interval,
                "from_date": from_date,
                "to_date": to_date,
                "stock_code": stock_code,
                "exchange_code": mapped_exchange,
            }
            if mapped_exchange in ("NFO", "BFO"):
                params = self._get_quotes_params_from_long_symbol(symbol, exchange)
                expiry_date = (params.get("expiry_date") or "").strip()
                if not expiry_date:
                    raise MarketDataError(
                        "Expiry date is required for F&O historical data; symbol must be in long_symbol format (e.g. NIFTY_OPT_YYYYMMDD_CALL_19500)",
                        create_error_context(symbol=symbol, exchange=mapped_exchange),
                    )
                kwargs["expiry_date"] = expiry_date
                kwargs["product_type"] = params.get("product_type") or "options"
                kwargs["right"] = params.get("right") or "others"
                kwargs["strike_price"] = str(params.get("strike_price") or "0")
            else:
                kwargs["product_type"] = "cash"

            resp = self.api.get_historical_data_v2(**kwargs)

            rows = resp.get("Success") if isinstance(resp, dict) else None
            if not isinstance(rows, list):
                raise MarketDataError(
                    "Invalid response received from IciciDirect historical API",
                    create_error_context(response=str(resp)[:1000]),
                )

            out: list[HistoricalData] = []
            for row in rows:
                ts = row.get("datetime") or row.get("time") or row.get("date")
                dt_val = _parse_api_date_to_kolkata(ts)
                out.append(
                    HistoricalData(
                        date=dt_val,
                        open=float(row.get("open", float("nan"))),
                        high=float(row.get("high", float("nan"))),
                        low=float(row.get("low", float("nan"))),
                        close=float(row.get("close", float("nan"))),
                        volume=int(float(row.get("volume", 0) or 0)),
                        intoi=int(float(row.get("open_interest", 0) or 0)),
                        oi=int(float(row.get("open_interest", 0) or 0)),
                    )
                )

            # For 1d periodicity, when date_end is today, update with today's OHLCV from intraday (like Shoonya).
            today_date = get_tradingapi_now().date()
            date_end_date = pd.to_datetime(date_end).date()
            if periodicity in ("1d", "D") and date_end_date >= today_date:
                today_str = get_tradingapi_now().strftime("%Y-%m-%d")
                from_date_today = f"{today_str}T00:00:00.000Z"
                to_date_today = f"{today_str}T23:59:59.000Z"
                kwargs_today = {
                    "interval": "1minute",
                    "from_date": from_date_today,
                    "to_date": to_date_today,
                    "stock_code": stock_code,
                    "exchange_code": mapped_exchange,
                }
                if mapped_exchange in ("NFO", "BFO"):
                    params = self._get_quotes_params_from_long_symbol(symbol, exchange)
                    kwargs_today["expiry_date"] = (params.get("expiry_date") or "").strip()
                    kwargs_today["product_type"] = params.get("product_type") or "options"
                    kwargs_today["right"] = params.get("right") or "others"
                    kwargs_today["strike_price"] = str(params.get("strike_price") or "0")
                else:
                    kwargs_today["product_type"] = "cash"
                try:
                    resp_today = self.api.get_historical_data_v2(**kwargs_today)
                    rows_today = resp_today.get("Success") if isinstance(resp_today, dict) else []
                    if isinstance(rows_today, list) and len(rows_today) > 0:
                        df_t = pd.DataFrame(rows_today)
                        ts_col = df_t["datetime"] if "datetime" in df_t.columns else (df_t["time"] if "time" in df_t.columns else df_t["date"])
                        df_t["date"] = pd.to_datetime(ts_col)
                        if df_t["date"].dt.tz is not None:
                            df_t["date"] = df_t["date"].dt.tz_convert(ICICIDIRECT_TIMEZONE)
                        else:
                            df_t["date"] = df_t["date"].dt.tz_localize(ICICIDIRECT_TIMEZONE)
                        df_t = df_t.set_index("date")
                        agg_map: Dict[str, str] = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
                        if "open_interest" in df_t.columns:
                            agg_map["open_interest"] = "last"
                        resampled = df_t.resample("D").agg(agg_map)  # type: ignore[arg-type]
                        for _, r in resampled.iterrows():
                            idx_val = r.name if r.name is not None else resampled.index[0]
                            dt_val = _parse_api_date_to_kolkata(
                                cast(Union[str, dt.datetime, pd.Timestamp], idx_val)
                            )
                            oi_val = int(float(r.get("open_interest", 0) or 0)) if "open_interest" in r.index else 0
                            today_bar = HistoricalData(
                                date=dt_val,
                                open=float(r.get("open", float("nan"))),
                                high=float(r.get("high", float("nan"))),
                                low=float(r.get("low", float("nan"))),
                                close=float(r.get("close", float("nan"))),
                                volume=int(float(r.get("volume", 0) or 0)),
                                intoi=oi_val,
                                oi=oi_val,
                            )
                            # Remove any existing bar for today from daily API, then append intraday-derived bar
                            def _day(d):
                                return d.date() if hasattr(d, "date") and callable(getattr(d, "date", None)) else pd.to_datetime(d).date()

                            out = [x for x in out if _day(x.date) != today_date]
                            out.append(today_bar)
                            break
                except Exception as e:
                    trading_logger.log_warning(
                        "Failed to fetch today intraday for 1d update",
                        {"symbol": symbol, "error": str(e)},
                    )

            out.sort(key=lambda x: x.date)
            return {symbol: out}
        except (ValidationError, SymbolError, MarketDataError):
            raise
        except Exception as e:
            raise MarketDataError(
                f"Error fetching historical for IciciDirect: {str(e)}",
                create_error_context(symbols=str(symbols), error=str(e)),
            )

    # ------------------------------------------------------------------
    # Orders / Positions
    # ------------------------------------------------------------------

    @log_execution_time
    @validate_inputs(order=lambda o: isinstance(o, Order))
    def place_order(self, order: Order, **kwargs) -> Order:
        """
        Place an order via BreezeConnect.

        This is a basic mapping; you will likely need to refine product/action/order_type
        mapping once you integrate it with real IciciDirect usage.
        """
        if self.api is None:
            raise BrokerConnectionError("IciciDirect not connected", create_error_context())

        try:
            mapped_exchange = self.map_exchange_for_api(order.long_symbol, order.exchange or "NSE")
            if mapped_exchange not in self.exchange_mappings:
                raise SymbolError(
                    f"Exchange {mapped_exchange} not available for IciciDirect",
                    create_error_context(
                        mapped_exchange=mapped_exchange,
                        available_exchanges=list(self.exchange_mappings.keys()),
                    ),
                )

            stock_code = self._resolve_stock_code_for_symbol(order.long_symbol, mapped_exchange)

            exchange_code = mapped_exchange

            if order.paper:
                order.exch_order_id = str(secrets.randbelow(10**15)) + "P"
                order.broker_order_id = str(secrets.randbelow(10**8)) + "P"
                order.orderRef = order.internal_order_id
                order.message = "Paper Order"
                order.status = OrderStatus.FILLED
                order.broker = Brokers.ICICIDIRECT
                order.additional_info = json.dumps({"paper": True, "broker": "ICICIDIRECT"})
                self._persist_order_links(order)
                return order

            order_side = str(order.order_type or "").upper().strip()
            if order_side in ["BUY", "COVER"]:
                action = "buy"
            elif order_side in ["SELL", "SHORT"]:
                action = "sell"
            else:
                raise ValidationError(
                    f"Invalid order_type for IciciDirect place_order: {order.order_type}",
                    create_error_context(order_type=order.order_type),
                )

            has_trigger = not math.isnan(order.trigger_price)
            has_limit_price = (not math.isnan(order.price)) and float(order.price) > 0
            if has_trigger:
                mapped_order_type = "stoploss"
            else:
                mapped_order_type = "limit" if has_limit_price else "market"

            symbol_params = self._get_quotes_params_from_long_symbol(order.long_symbol, order.exchange or "NSE")
            product = symbol_params.get("product_type") or "cash"
            payload: Dict[str, object] = {
                "stock_code": str(stock_code),
                "exchange_code": exchange_code,
                "product": product,
                "action": action,
                "order_type": mapped_order_type,
                "quantity": str(int(order.quantity)),
                "price": str(order.price if has_limit_price else 0),
                "validity": str(kwargs.get("validity", "day")).lower(),
                "user_remark": self._sanitize_user_remark(str(kwargs.get("user_remark") or order.internal_order_id or "")),
            }

            if has_trigger:
                payload["stoploss"] = str(order.trigger_price)

            if product in ("options", "futures"):
                payload["expiry_date"] = str(symbol_params.get("expiry_date") or kwargs.get("expiry_date") or "")
                payload["right"] = str(symbol_params.get("right") or kwargs.get("right") or ("others" if product == "futures" else ""))
                payload["strike_price"] = str(symbol_params.get("strike_price") or kwargs.get("strike_price") or ("0" if product == "futures" else ""))

            # Allow explicitly provided kwargs only when accepted by installed SDK signature.
            payload.update({k: v for k, v in kwargs.items() if v is not None})
            if "product_type" in payload and "product" not in kwargs:
                payload["product"] = payload.get("product_type") or product
            resp = self._call_api_method("place_order", payload)

            success = resp.get("Success", {}) if isinstance(resp, dict) else {}
            success_dict = success if isinstance(success, dict) else {}
            error = resp.get("Error") if isinstance(resp, dict) else None
            status_code = resp.get("Status") if isinstance(resp, dict) else None
            broker_order_id = (
                success_dict.get("order_id")
                or success_dict.get("order_no")
                or success_dict.get("order_reference")
                or success_dict.get("orderNumber")
            )

            order.broker_order_id = str(broker_order_id or "")
            order.exch_order_id = str(success_dict.get("exchange_order_id") or success_dict.get("exchangeOrderID") or "")
            order.orderRef = order.internal_order_id
            order.status = OrderStatus.PENDING if order.broker_order_id else OrderStatus.REJECTED
            error_message = ""
            if error not in [None, ""]:
                error_message = str(error)
            elif success_dict:
                error_message = str(success_dict.get("message") or success_dict.get("Error") or "")
            elif isinstance(resp, dict):
                error_message = str(resp.get("Message") or "")
            if status_code not in [None, ""] and error_message:
                error_message = f"[Status {status_code}] {error_message}"
            order.message = error_message
            order.additional_info = json.dumps({"raw_response": resp})

            if order.broker_order_id:
                self._persist_order_links(order)
                update_order_status(self, order.internal_order_id, order.broker_order_id, eod=False)
            return order
        except Exception as e:
            context = create_error_context(
                long_symbol=order.long_symbol,
                order_type=order.order_type,
                quantity=order.quantity,
                error=str(e),
            )
            raise OrderError(f"Error placing order via IciciDirect: {str(e)}", context)

    def modify_order(self, **kwargs) -> Order:
        if self.api is None:
            raise BrokerConnectionError("IciciDirect not connected", create_error_context())

        try:
            mandatory_keys = ["broker_order_id", "new_price", "new_quantity"]
            missing_keys = [key for key in mandatory_keys if key not in kwargs]
            if missing_keys:
                raise ValidationError(
                    f"Missing mandatory keys: {', '.join(missing_keys)}",
                    create_error_context(kwargs=kwargs, missing_keys=missing_keys),
                )

            broker_order_id = str(kwargs.get("broker_order_id") or kwargs.get("order_id") or "").strip()
            if not broker_order_id:
                raise ValidationError("broker_order_id/order_id is required", create_error_context(kwargs=kwargs))

            new_price = float(kwargs.get("new_price", kwargs.get("price", 0)) or 0)
            new_quantity = int(float(kwargs.get("new_quantity", kwargs.get("quantity", 0)) or 0))

            if new_price < 0:
                raise ValidationError("new_price cannot be negative", create_error_context(new_price=new_price))
            if new_quantity <= 0:
                raise ValidationError("new_quantity must be greater than 0", create_error_context(new_quantity=new_quantity))

            redis_order_data = self.redis_o.hgetall(broker_order_id)
            o = kwargs.get("order")
            order: Order = o if isinstance(o, Order) else (Order(**redis_order_data) if redis_order_data else Order())
            order.broker_order_id = broker_order_id
            order.broker = self.broker

            if broker_order_id.upper().endswith("P"):
                order.price = new_price
                order.quantity = new_quantity
                order.status = OrderStatus.FILLED
                order.message = "Paper Order (modify simulated)"
                order.additional_info = json.dumps({"paper": True, "operation": "modify", "request": kwargs})
                self.redis_o.hmset(broker_order_id, {key: str(val) for key, val in order.to_dict().items()})
                return order

            current_info = self.get_order_info(broker_order_id=broker_order_id)
            if current_info.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
                order.status = current_info.status
                order.message = f"Order not modifiable in status {current_info.status.name}"
                return order

            exchange_code = str(kwargs.get("exchange_code") or self._resolve_exchange_code_for_order(broker_order_id))
            mod_order_type = kwargs.get("order_type")
            if mod_order_type is None:
                mod_order_type = "limit" if new_price > 0 else "market"

            payload: Dict[str, object] = {
                "order_id": broker_order_id,
                "exchange_code": exchange_code,
                "order_type": str(mod_order_type or ""),
                "stoploss": kwargs.get("stoploss", ""),
                "quantity": str(new_quantity),
                "price": str(new_price),
                "validity": kwargs.get("validity", ""),
                "disclosed_quantity": kwargs.get("disclosed_quantity", ""),
                "validity_date": kwargs.get("validity_date", ""),
            }
            resp = self._call_api_method("modify_order", payload)

            success = resp.get("Success", {}) if isinstance(resp, dict) else {}
            success_dict = success if isinstance(success, dict) else {}
            error = resp.get("Error") if isinstance(resp, dict) else None
            status_code = resp.get("Status") if isinstance(resp, dict) else None

            order.price = new_price
            order.quantity = new_quantity
            order.status = OrderStatus.PENDING if error in [None, ""] else OrderStatus.REJECTED
            error_message = ""
            if error not in [None, ""]:
                error_message = str(error)
            elif success_dict:
                error_message = str(success_dict.get("message") or success_dict.get("Error") or "")
            elif isinstance(resp, dict):
                error_message = str(resp.get("Message") or "")
            if status_code not in [None, ""] and error_message:
                error_message = f"[Status {status_code}] {error_message}"
            order.message = error_message
            order.additional_info = json.dumps({"raw_response": resp})

            self.redis_o.hmset(broker_order_id, {key: str(val) for key, val in order.to_dict().items()})
            if order.internal_order_id:
                refreshed = update_order_status(self, order.internal_order_id, broker_order_id, eod=False)
                if refreshed is not None:
                    order.status = refreshed.status
                    order.exch_order_id = refreshed.exchange_order_id
                    if float(refreshed.fill_price or 0) > 0:
                        order.price = float(refreshed.fill_price)
            return order
        except (ValidationError, BrokerConnectionError):
            raise
        except Exception as e:
            raise OrderError(f"Error modifying IciciDirect order: {str(e)}", create_error_context(kwargs=kwargs, error=str(e)))

    def cancel_order(self, **kwargs) -> Order:
        if self.api is None:
            raise BrokerConnectionError("IciciDirect not connected", create_error_context())

        try:
            broker_order_id = kwargs.get("broker_order_id") or kwargs.get("order_id")
            if not broker_order_id:
                raise ValidationError("broker_order_id/order_id is required", create_error_context(kwargs=kwargs))

            broker_order_id = str(broker_order_id)
            o = kwargs.get("order")
            order: Order = o if isinstance(o, Order) else Order()
            order.broker = self.broker

            if broker_order_id.endswith("P"):
                order.broker_order_id = broker_order_id
                order.status = OrderStatus.CANCELLED
                order.message = "Paper Order (cancel simulated)"
                order.additional_info = json.dumps({"paper": True, "operation": "cancel", "request": kwargs})
                return order

            exchange_code = str(kwargs.get("exchange_code") or self._resolve_exchange_code_for_order(broker_order_id))
            payload = {
                "exchange_code": exchange_code,
                "order_id": broker_order_id,
            }
            resp = self._call_api_method("cancel_order", payload)

            order.broker_order_id = broker_order_id
            order.status = OrderStatus.CANCELLED
            order.message = str(resp.get("Error") if isinstance(resp, dict) else "")
            order.additional_info = json.dumps({"raw_response": resp})
            return order
        except (ValidationError, BrokerConnectionError):
            raise
        except Exception as e:
            raise OrderError(f"Error cancelling IciciDirect order: {str(e)}", create_error_context(kwargs=kwargs, error=str(e)))

    def get_order_info(self, **kwargs) -> OrderInfo:
        if self.api is None:
            raise BrokerConnectionError("IciciDirect not connected", create_error_context())

        try:
            order_id = str(kwargs.get("broker_order_id") or kwargs.get("order_id") or "")
            if not order_id:
                raise ValidationError("broker_order_id/order_id is required", create_error_context(kwargs=kwargs))

            # Paper orders are simulated locally and do not exist in broker order book APIs.
            if order_id.upper().endswith("P"):
                redis_rec = self.redis_o.hgetall(order_id) if hasattr(self, "redis_o") else {}
                q = int(float(redis_rec.get("quantity", 0) or 0))
                p = float(redis_rec.get("price", 0) or 0)
                return OrderInfo(
                    order_size=q,
                    order_price=p,
                    fill_size=q,
                    fill_price=p,
                    status=OrderStatus.FILLED,
                    broker_order_id=order_id,
                    exchange_order_id=str(redis_rec.get("exch_order_id", order_id)),
                    broker=self.broker,
                )

            book = self.get_orders_today()
            if book.empty:
                raise OrderError(f"Order book is empty while searching for {order_id}", create_error_context(order_id=order_id))
            if "order_id" not in book.columns:
                raise OrderError(
                    f"order_id column missing in order book while searching for {order_id}",
                    create_error_context(order_id=order_id, columns=list(book.columns)),
                )

            row = book[book["order_id"].astype(str) == order_id]
            if row.empty:
                raise OrderError(f"Order {order_id} not found in order book", create_error_context(order_id=order_id))

            rec = row.iloc[-1]

            order_size = int(float(rec.get("quantity", rec.get("order_qty", 0)) or 0))
            fill_size = int(
                float(
                    rec.get("filled_quantity", rec.get("executed_quantity", rec.get("filled_qty", 0))) or 0
                )
            )
            order_price = float(rec.get("price", rec.get("order_price", float("nan"))))
            fill_price = float(rec.get("average_price", rec.get("fill_price", 0)) or 0)
            exchange_order_id = str(
                rec.get("exchange_order_id", rec.get("exchangeOrderID", rec.get("exch_order_id", "")))
            )
            status_raw = str(rec.get("status", rec.get("order_status", "")))
            status = self._normalize_order_status(status_raw, fill_size, order_size, exchange_order_id)

            return OrderInfo(
                order_size=order_size,
                order_price=order_price,
                fill_size=fill_size,
                fill_price=fill_price,
                status=status,
                broker_order_id=order_id,
                exchange_order_id=exchange_order_id,
                broker=self.broker,
            )
        except (ValidationError, BrokerConnectionError, OrderError):
            raise
        except Exception as e:
            raise OrderError(f"Error fetching IciciDirect order info: {str(e)}", create_error_context(kwargs=kwargs, error=str(e)))

    def get_position(self, long_symbol: str) -> Union[pd.DataFrame, int]:
        if self.api is None:
            raise BrokerConnectionError("IciciDirect not connected", create_error_context())

        try:
            resp = self.api.get_portfolio_positions()
            rows = resp.get("Success") if isinstance(resp, dict) else None
            df = pd.DataFrame(rows or [])
            if long_symbol:
                if df.empty:
                    return 0
                key = "stock_code" if "stock_code" in df.columns else ("symbol" if "symbol" in df.columns else None)
                if key:
                    df = df[df[key].astype(str).str.upper() == str(long_symbol).split("_")[0].upper()]
                return df if not df.empty else 0
            return df
        except Exception as e:
            raise MarketDataError(f"Error fetching IciciDirect positions: {str(e)}", create_error_context(error=str(e)))

    def get_orders_today(self, **kwargs) -> pd.DataFrame:
        if self.api is None:
            raise BrokerConnectionError("IciciDirect not connected", create_error_context())

        try:
            from_date, to_date = self._today_window_utc()
            req_from = kwargs.get("from_date", from_date)
            req_to = kwargs.get("to_date", to_date)
            exchange_codes = [str(kwargs["exchange_code"]).upper()] if kwargs.get("exchange_code") else ["NSE", "BSE", "NFO", "BFO"]
            frames: list[pd.DataFrame] = []

            for exchange_code in exchange_codes:
                resp = self._call_api_method(
                    "get_order_list",
                    {
                        "exchange_code": exchange_code,
                        "from_date": req_from,
                        "to_date": req_to,
                    },
                )
                rows = resp.get("Success") if isinstance(resp, dict) else None
                if isinstance(rows, list) and rows:
                    frames.append(pd.DataFrame(rows))

            df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            # normalize core columns used by get_order_info
            rename_map = {
                "order_no": "order_id",
                "status": "status",
                "quantity": "quantity",
                "pending_quantity": "pending_quantity",
                "executed_quantity": "filled_quantity",
                "average_price": "average_price",
                "price": "price",
                "exchange_order_id": "exchange_order_id",
            }
            present = {k: v for k, v in rename_map.items() if k in df.columns}
            if present:
                df = df.rename(columns=present)
            return df
        except Exception as e:
            raise OrderError(f"Error fetching IciciDirect orders: {str(e)}", create_error_context(error=str(e), kwargs=kwargs))

    def get_trades_today(self, **kwargs) -> pd.DataFrame:
        if self.api is None:
            raise BrokerConnectionError("IciciDirect not connected", create_error_context())

        try:
            from_date, to_date = self._today_window_utc()
            req_from = kwargs.get("from_date", from_date)
            req_to = kwargs.get("to_date", to_date)
            exchange_codes = [str(kwargs["exchange_code"]).upper()] if kwargs.get("exchange_code") else ["NSE", "BSE", "NFO", "BFO"]

            frames: list[pd.DataFrame] = []
            for exchange_code in exchange_codes:
                payload: Dict[str, object] = {
                    "from_date": req_from,
                    "to_date": req_to,
                    "exchange_code": exchange_code,
                    "product_type": kwargs.get("product_type", ""),
                    "action": kwargs.get("action", ""),
                    "stock_code": kwargs.get("stock_code", ""),
                }
                resp = self._call_api_method("get_trade_list", payload)
                rows = resp.get("Success") if isinstance(resp, dict) else None
                if isinstance(rows, list) and rows:
                    frames.append(pd.DataFrame(rows))
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        except Exception as e:
            raise MarketDataError(f"Error fetching IciciDirect trades: {str(e)}", create_error_context(error=str(e), kwargs=kwargs))

    def get_long_name_from_broker_identifier(self, **kwargs) -> pd.Series:
        broker_identifier = str(kwargs.get("broker_identifier") or kwargs.get("scrip_code") or "")
        if not broker_identifier:
            raise ValidationError("broker_identifier or scrip_code is required", create_error_context(kwargs=kwargs))

        try:
            if self.codes is None or self.codes.empty:
                self.update_symbology(saveToFolder=False)

            matches = self.codes[self.codes["Scripcode"].astype(str) == broker_identifier]
            return matches["long_symbol"] if not matches.empty else pd.Series(dtype="object")
        except Exception as e:
            raise SymbolError(
                f"Error mapping broker identifier to long_symbol: {str(e)}",
                create_error_context(broker_identifier=broker_identifier, error=str(e)),
            )

    def get_min_lot_size(self, long_symbol: str, exchange: str) -> int:
        try:
            mapped_exchange = self.map_exchange_for_api(long_symbol, exchange)
            if mapped_exchange in self.exchange_mappings:
                lot = self.exchange_mappings[mapped_exchange]["contractsize_map"].get(long_symbol)
                if lot is not None:
                    return int(lot)

            if self.codes is None or self.codes.empty:
                self.update_symbology(saveToFolder=False)

            row = self.codes[self.codes["long_symbol"] == long_symbol]
            if row.empty:
                raise SymbolError(
                    f"Unable to find lot size for {long_symbol}",
                    create_error_context(long_symbol=long_symbol, exchange=exchange),
                )
            return int(float(row.iloc[0].get("LotSize", 1)))
        except SymbolError:
            raise
        except Exception as e:
            raise SymbolError(
                f"Error getting lot size from IciciDirect: {str(e)}",
                create_error_context(long_symbol=long_symbol, exchange=exchange, error=str(e)),
            )

    def get_available_capital(self) -> Dict[str, float]:
        if self.api is None:
            raise BrokerConnectionError("IciciDirect not connected", create_error_context())

        try:
            resp = self.api.get_funds()
            data = resp.get("Success", {}) if isinstance(resp, dict) else {}
            if isinstance(data, list):
                data = data[0] if data else {}

            cash = float(data.get("available_margin", data.get("cash_limit", data.get("cash", 0))) or 0)
            collateral = float(data.get("collateral", data.get("adhoc_margin", 0)) or 0)
            return {"cash": cash, "collateral": collateral}
        except Exception as e:
            raise MarketDataError(
                f"Error fetching IciciDirect available capital: {str(e)}",
                create_error_context(error=str(e)),
            )
