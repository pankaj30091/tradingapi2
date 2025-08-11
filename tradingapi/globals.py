import datetime as dt

# This will hold the override value, or None if not set
TRADINGAPI_NOW = None  # dt.datetime object


def get_tradingapi_now() -> dt.datetime:
    """Return the global datetime override, or dt.datetime.now() if not set."""
    global TRADINGAPI_NOW
    return TRADINGAPI_NOW if TRADINGAPI_NOW is not None else dt.datetime.now()
