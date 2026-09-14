"""Default network denial for every Pattern Lab test.

Collector cases inject a synthetic OKX/Bybit protocol fixture through the
adapters' transport boundary.  This guard closes the remaining hole: if an
injection is ever forgotten, the default transport and the underlying
``urllib.request.urlopen`` both fail loudly instead of reaching a real exchange.

The guard is installed with plain attribute assignment rather than through
``monkeypatch``, so a test's own ``monkeypatch.undo()`` restores the guard, never
the real socket path.
"""

from __future__ import annotations

import urllib.request

import pytest

from tools.pattern_lab import exchange_data

NETWORK_DENIED = (
    "Pattern Lab tests must inject a synthetic transport: a real exchange request was attempted."
)


def _forbidden(*args, **kwargs):
    raise AssertionError(NETWORK_DENIED)


@pytest.fixture(autouse=True)
def deny_real_network():
    """Refuse any request that was not injected through the transport boundary."""
    saved_transport = exchange_data.urllib_transport
    saved_urlopen = urllib.request.urlopen
    exchange_data.urllib_transport = _forbidden
    urllib.request.urlopen = _forbidden
    try:
        yield
    finally:
        exchange_data.urllib_transport = saved_transport
        urllib.request.urlopen = saved_urlopen
