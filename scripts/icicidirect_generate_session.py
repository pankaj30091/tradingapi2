#!/usr/bin/env python3
"""
Non-interactive ICICIDirect/Breeze session-token fetcher.

This script automates the login redirect flow and prints only the generated
session token to stdout so it can be used by:

    ICICIDIRECT.AUTO_SESSION_TOKEN_CMD

Example:
    python scripts/icicidirect_generate_session.py \
        --api-key "$ICICI_API_KEY" \
        --user-id "$ICICI_USER_ID" \
        --password "$ICICI_PASSWORD" \
        --totp-token "$ICICI_TOTP_TOKEN"

Notes:
- ICICI login pages may change often; selectors are configurable via flags.
- If broker enforces CAPTCHA/manual challenge, fully headless automation may fail.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from typing import Any, Iterable, Optional
from urllib.parse import parse_qs, urlparse

LOGIN_URL_TEMPLATE = "https://api.icicidirect.com/apiuser/login?api_key={api_key}"
TOKEN_QUERY_KEYS = ("api_session", "apisession", "session_token")


def first_present(driver: Any, selectors: Iterable[str], timeout_s: int = 8):
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    for selector in selectors:
        try:
            return WebDriverWait(driver, timeout_s).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
        except TimeoutException:
            continue
    return None


def first_clickable(driver: Any, selectors: Iterable[str], timeout_s: int = 8):
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    for selector in selectors:
        try:
            return WebDriverWait(driver, timeout_s).until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
        except TimeoutException:
            continue
    return None


def parse_session_token_from_url(url: str) -> Optional[str]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    for key in TOKEN_QUERY_KEYS:
        vals = query.get(key)
        if vals and vals[0].strip():
            return vals[0].strip()

    # fallback: look in URL text if query parser misses odd redirects
    for key in TOKEN_QUERY_KEYS:
        m = re.search(rf"(?:[?&]|\b){key}=([^&\s]+)", url)
        if m:
            return m.group(1).strip()
    return None


def generate_session_token(args: argparse.Namespace) -> str:
    try:
        from selenium import webdriver
        from selenium.webdriver.firefox.service import Service as FirefoxService
    except Exception as exc:
        raise RuntimeError(f"selenium is required but not available: {exc}") from exc

    login_url = args.login_url or LOGIN_URL_TEMPLATE.format(api_key=args.api_key)

    options = webdriver.FirefoxOptions()
    if args.headless:
        options.add_argument("--headless")

    service = None
    if args.driver_path:
        driver_path = args.driver_path
        if os.path.isdir(driver_path):
            candidate = os.path.join(driver_path, "geckodriver")
            if os.path.exists(candidate):
                driver_path = candidate
        service = FirefoxService(executable_path=driver_path)

    driver = webdriver.Firefox(options=options, service=service)
    driver.set_page_load_timeout(args.page_load_timeout)

    try:
        driver.get(login_url)

        user_el = first_present(driver, args.user_selectors)
        if user_el is None:
            raise RuntimeError("Unable to locate user-id input field")
        user_el.clear()
        user_el.send_keys(args.user_id)

        password_el = first_present(driver, args.password_selectors)
        if password_el is None:
            raise RuntimeError("Unable to locate password input field")
        password_el.clear()
        password_el.send_keys(args.password)

        submit_el = first_clickable(driver, args.submit_selectors)
        if submit_el is None:
            raise RuntimeError("Unable to locate initial submit/login button")
        if args.tnc_selector:
            tnc_el = first_clickable(driver, [args.tnc_selector], timeout_s=3)
            if tnc_el is not None:
                tnc_el.click()
        submit_el.click()

        # If OTP step appears, fill TOTP.
        otp_el = first_present(driver, args.otp_selectors, timeout_s=args.otp_wait)
        if otp_el is not None:
            if not args.totp_token:
                raise RuntimeError("OTP step detected but --totp-token is not provided")
            try:
                import pyotp
            except Exception as exc:
                raise RuntimeError(f"pyotp is required for OTP flow but is not available: {exc}") from exc
            otp = pyotp.TOTP(args.totp_token).now()
            otp_el.clear()
            otp_el.send_keys(otp)

            otp_submit_el = first_clickable(driver, args.otp_submit_selectors)
            if otp_submit_el is None:
                raise RuntimeError("Unable to locate OTP submit button")
            otp_submit_el.click()

        deadline = time.time() + args.redirect_wait
        token = None
        while time.time() < deadline:
            token = parse_session_token_from_url(driver.current_url)
            if token:
                break
            time.sleep(1)

        if not token:
            raise RuntimeError(
                "Could not extract session token from redirect URL. "
                "If login page changed, pass custom selectors; if CAPTCHA is enabled, manual intervention may be required."
            )

        return token
    finally:
        driver.quit()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate ICICIDirect API session token via automated login")

    parser.add_argument("--api-key", required=True)
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--totp-token", default="")
    parser.add_argument("--login-url", default="")
    parser.add_argument("--driver-path", default="")
    parser.add_argument("--tnc-selector", default="")

    parser.add_argument("--headless", dest="headless", action="store_true")
    parser.add_argument("--no-headless", dest="headless", action="store_false")
    parser.set_defaults(headless=True)
    parser.add_argument("--redirect-wait", type=int, default=45)
    parser.add_argument("--otp-wait", type=int, default=12)
    parser.add_argument("--page-load-timeout", type=int, default=45)

    parser.add_argument(
        "--user-selectors",
        nargs="+",
        default=["#userid", "#txtuid", "input[name='user_id']", "input[type='text']"],
    )
    parser.add_argument(
        "--password-selectors",
        nargs="+",
        default=["#password", "#txtPass", "input[name='password']", "input[type='password']"],
    )
    parser.add_argument(
        "--submit-selectors",
        nargs="+",
        default=["button[type='submit']", "#btn_login", "#btnLogin", "input[type='submit']"],
    )
    parser.add_argument(
        "--otp-selectors",
        nargs="+",
        default=["#otp", "#txtOTP", "input[name='otp']", "input[autocomplete='one-time-code']"],
    )
    parser.add_argument(
        "--otp-submit-selectors",
        nargs="+",
        default=["#btn_verify_otp", "#btnSubmitOTP", "button[type='submit']", "input[type='submit']"],
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        token = generate_session_token(args)
        print(token)
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
