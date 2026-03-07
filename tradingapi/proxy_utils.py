"""
Proxy utilities for broker HTTP requests.

When a broker has USE_PROXY=True in config and proxy settings (e.g. Webshare) are
configured, get_proxies_for_broker() returns a proxies dict for use with requests.
Used in connect() and save_symbol_data() for each broker.
"""
import logging
import os
import random
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

_WEBSHARE_LIST_URL = "https://proxy.webshare.io/api/v2/proxy/list/"


def get_proxies_for_broker(broker_name: str) -> Optional[Dict[str, str]]:
    """
    Return a proxies dict for use with requests if this broker has USE_PROXY enabled
    and proxy config (e.g. Webshare) is present. Otherwise return None.

    Returns:
        {"http": "http://user:pass@host:port", "https": "http://user:pass@host:port"}
        or None if proxy should not be used.
    """
    try:
        from .config import get_config

        config = get_config()
    except Exception:
        return None

    use_proxy = config.get(f"{broker_name}.USE_PROXY")
    if not use_proxy:
        return None

    proxy_cfg = config.configs.get("proxy")
    if not isinstance(proxy_cfg, dict):
        logger.debug("Proxy not used: no proxy section in config", extra={"broker": broker_name})
        return None

    source = (proxy_cfg.get("source") or "").strip().lower()
    if source != "webshare":
        logger.debug("Proxy not used: only webshare source is supported", extra={"broker": broker_name})
        return None

    api_key = proxy_cfg.get("api_key") or os.getenv("WEBSHARE_PROXY_API_KEY")
    if not api_key:
        logger.warning("Proxy enabled but proxy.api_key not set", extra={"broker": broker_name})
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
            logger.warning(
                "Webshare proxy list failed",
                extra={"broker": broker_name, "status_code": resp.status_code},
            )
            return None
        data = resp.json()
        results = data.get("results") or []
        valid_proxies = [p for p in results if p.get("valid")]
        if not valid_proxies:
            logger.warning("No valid Webshare proxies in list", extra={"broker": broker_name})
            return None
        proxy = random.choice(valid_proxies)
        host = proxy.get("proxy_address", "")
        port = proxy.get("port", 80)
        username = proxy.get("username") or proxy_cfg.get("username")
        password = proxy.get("password") or proxy_cfg.get("password")
        if username and password:
            proxy_url = f"http://{username}:{password}@{host}:{port}"
        else:
            proxy_url = f"http://{host}:{port}"
        return {"http": proxy_url, "https": proxy_url}
    except Exception as e:
        logger.warning(
            "Failed to fetch proxy for broker",
            extra={"broker": broker_name, "error": str(e)},
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
