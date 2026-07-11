"""
Proxy utilities for broker HTTP requests.

When a broker has USE_PROXY=True (API) or USE_PROXY_SYMBOL_DOWNLOAD=True (symbol master),
configured, get_proxies_for_broker() returns a proxies dict for use with requests.
Used in connect() and save_symbol_data() for each broker.
"""
import os
import random
from typing import Any, Dict, Optional, Tuple, Union
from urllib.parse import quote

import requests

from tradingapi import trading_logger

_WEBSHARE_LIST_URL = "https://proxy.webshare.io/api/v2/proxy/list/"


def _get_proxy_credentials(proxy_cfg: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    username = proxy_cfg.get("user") or proxy_cfg.get("username")
    password = proxy_cfg.get("pass") or proxy_cfg.get("password")
    return username, password


def _get_proxy_config(broker_name: str, purpose: str = "api") -> Optional[tuple[Any, Dict[str, Any]]]:
    try:
        from .config import get_config

        config = get_config()
    except Exception:
        return None

    if purpose == "symbol_download":
        use_proxy = config.get(f"{broker_name}.USE_PROXY_SYMBOL_DOWNLOAD")
    else:
        use_proxy = config.get(f"{broker_name}.USE_PROXY")
    if not use_proxy:
        return None

    proxy_cfg = config.configs.get("proxy")
    if not isinstance(proxy_cfg, dict):
        trading_logger.log_debug("Proxy not used: no proxy section in config", context={"broker": broker_name})
        return None

    return config, proxy_cfg


def get_proxy_verify_ssl(broker_name: str, purpose: str = "api") -> bool:
    """Return False when broker proxy requires disabled SSL verify (e.g. NordVPN)."""
    cfg = _get_proxy_config(broker_name, purpose=purpose)
    if not cfg:
        return True
    _, proxy_cfg = cfg
    source = (proxy_cfg.get("source") or "").strip().lower()
    return source != "nordvpn"


def _nordvpn_max_proxies(proxy_cfg: Dict[str, Any]) -> int:
    try:
        return max(1, int(proxy_cfg.get("max_proxies") or 30))
    except (TypeError, ValueError):
        return 30


def _build_nordvpn_proxies(proxy_cfg: Dict[str, Any], broker_name: str) -> list[Dict[str, str]]:
    username, password = _get_proxy_credentials(proxy_cfg)
    if not username or not password:
        trading_logger.log_warning(
            "Proxy enabled but proxy user/pass not set for NordVPN",
            context={"broker": broker_name},
        )
        return []
    country_code = proxy_cfg.get("country_code") or "IN"
    try:
        from chameli.interactions import get_nordvpn_proxies

        host_ports = get_nordvpn_proxies(country_code=country_code, max_proxies=_nordvpn_max_proxies(proxy_cfg))
        u = quote(str(username), safe="")
        p = quote(str(password), safe="")
        return [
            {"http": f"https://{u}:{p}@{host_port}", "https": f"https://{u}:{p}@{host_port}"}
            for host_port in host_ports
        ]
    except Exception as e:
        trading_logger.log_warning(
            "Failed to fetch NordVPN proxy for broker",
            context={"broker": broker_name, "error": str(e)},
        )
        return []


def iter_proxies_for_broker(broker_name: str, purpose: str = "api"):
    """
    Yield (proxies_dict, verify_ssl) tuples to try for HTTP requests.
    NordVPN yields multiple servers; others yield at most one entry.
    When proxy is disabled, yields a single direct request config.
    """
    cfg = _get_proxy_config(broker_name, purpose=purpose)
    if not cfg:
        yield {}, True
        return

    _, proxy_cfg = cfg
    source = (proxy_cfg.get("source") or "webshare").strip().lower()
    if source == "nordvpn":
        proxies_list = _build_nordvpn_proxies(proxy_cfg, broker_name)
        if not proxies_list:
            yield {}, True
            return
        for proxies in proxies_list:
            yield proxies, False
        return

    proxies = get_proxies_for_broker(broker_name, purpose=purpose)
    if proxies:
        yield proxies, get_proxy_verify_ssl(broker_name, purpose=purpose)
    else:
        yield {}, True


def suppress_insecure_request_warnings() -> None:
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def request_get_with_broker_proxy(
    url: str,
    broker_name: str,
    purpose: str = "symbol_download",
    headers: Optional[Dict[str, str]] = None,
    timeout: Union[float, Tuple[float, float]] = (10, 300),
    min_content_bytes: int = 100_000,
) -> requests.Response:
    """
    GET url using broker proxy settings, rotating NordVPN servers until a valid response.
    Validates status 200 and minimum response size (Dhan 403 pages are ~919 bytes).
    """
    suppress_insecure_request_warnings()
    response = None
    last_status = None
    last_error = None
    for proxies, verify in iter_proxies_for_broker(broker_name, purpose=purpose):
        proxy_host = (proxies or {}).get("https", "").split("@")[-1] if proxies else "direct"
        try:
            response = requests.get(
                url,
                headers=headers or {},
                proxies=proxies or {},
                verify=verify,
                timeout=timeout,
            )
            last_status = response.status_code
            if response.status_code == 200 and len(response.content) >= min_content_bytes:
                if proxy_host and proxy_host != "direct":
                    trading_logger.log_info(
                        "HTTP request succeeded via proxy",
                        context={"broker": broker_name, "proxy": proxy_host, "url": url},
                    )
                return response
            trading_logger.log_warning(
                "Proxy HTTP attempt rejected or too small",
                context={
                    "broker": broker_name,
                    "proxy": proxy_host or "direct",
                    "status_code": response.status_code,
                    "content_bytes": len(response.content),
                },
            )
        except requests.RequestException as exc:
            last_error = str(exc)
            trading_logger.log_warning(
                "Proxy HTTP attempt failed",
                context={"broker": broker_name, "proxy": proxy_host or "direct", "error": last_error},
            )

    detail = f"status={last_status}" if last_status is not None else last_error or "unknown error"
    raise requests.RequestException(f"All proxy attempts failed for {broker_name}: {detail}")


def get_proxies_for_broker(broker_name: str, purpose: str = "api") -> Optional[Dict[str, str]]:
    """
    Return a proxies dict for use with requests if proxy is enabled for the given purpose
    and proxy config (e.g. Webshare, NordVPN) is present. Otherwise return None.

    purpose:
        "api" – controlled by {broker}.USE_PROXY (connect, trading API calls)
        "symbol_download" – controlled by {broker}.USE_PROXY_SYMBOL_DOWNLOAD (save_symbol_data)

    Returns:
        {"http": "http://user:pass@host:port", "https": "http://user:pass@host:port"}
        or None if proxy should not be used.
    """
    cfg = _get_proxy_config(broker_name, purpose=purpose)
    if not cfg:
        return None
    _, proxy_cfg = cfg

    source = (proxy_cfg.get("source") or "webshare").strip().lower()
    if source == "nordvpn":
        proxies_list = _build_nordvpn_proxies(proxy_cfg, broker_name)
        return proxies_list[0] if proxies_list else None

    if source != "webshare":
        trading_logger.log_debug(
            "Proxy not used: unsupported proxy source",
            context={"broker": broker_name, "source": source},
        )
        return None

    api_key = proxy_cfg.get("api_key") or os.getenv("WEBSHARE_PROXY_API_KEY")
    if not api_key:
        trading_logger.log_warning("Proxy enabled but proxy.api_key not set", context={"broker": broker_name})
        return None

    mode = proxy_cfg.get("mode") or "direct"
    country_code = proxy_cfg.get("country_code")
    params: Dict[str, Any] = {"mode": mode, "page": 1, "page_size": 100}
    if country_code:
        params["country_code"] = country_code

    try:
        resp = requests.get(
            _WEBSHARE_LIST_URL,
            params=params,
            headers={"Authorization": api_key},
            timeout=15,
        )
        if resp.status_code != 200:
            trading_logger.log_warning(
                "Webshare proxy list failed",
                context={"broker": broker_name, "status_code": resp.status_code},
            )
            return None
        data = resp.json()
        results = data.get("results") or []
        valid_proxies = [p for p in results if p.get("valid")]
        if not valid_proxies:
            trading_logger.log_warning("No valid Webshare proxies in list", context={"broker": broker_name})
            return None
        proxy = random.choice(valid_proxies)
        host = proxy.get("proxy_address", "")
        port = proxy.get("port", 80)
        username, password = _get_proxy_credentials(proxy_cfg)
        username = username or proxy.get("username")
        password = password or proxy.get("password")
        if username and password:
            proxy_url = f"http://{username}:{password}@{host}:{port}"
        else:
            proxy_url = f"http://{host}:{port}"
        return {"http": proxy_url, "https": proxy_url}
    except Exception as e:
        trading_logger.log_warning(
            "Failed to fetch proxy for broker",
            context={"broker": broker_name, "error": str(e)},
        )
        return None


def set_proxy_env_for_broker(broker_name: str) -> Optional[Dict[str, Optional[str]]]:
    """
    If broker has USE_PROXY enabled, set HTTP_PROXY and HTTPS_PROXY in the environment
    from Webshare and return the previous env values (so caller can restore).
    Otherwise return None and do not change env.
    """
    proxies = get_proxies_for_broker(broker_name)
    if not proxies:
        return None
    proxy_url = proxies.get("https") or proxies.get("http")
    if not proxy_url:
        return None
    old = {
        "HTTP_PROXY": os.environ.get("HTTP_PROXY"),
        "HTTPS_PROXY": os.environ.get("HTTPS_PROXY"),
        "http_proxy": os.environ.get("http_proxy"),
        "https_proxy": os.environ.get("https_proxy"),
    }
    os.environ["HTTP_PROXY"] = proxy_url
    os.environ["HTTPS_PROXY"] = proxy_url
    os.environ["http_proxy"] = proxy_url
    os.environ["https_proxy"] = proxy_url
    return old


def restore_proxy_env(previous: Optional[Dict[str, Optional[str]]]) -> None:
    """Restore HTTP_PROXY/HTTPS_PROXY env from a dict returned by set_proxy_env_for_broker."""
    if not previous:
        return
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
