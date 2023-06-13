"""Tests standard tap features using the built-in SDK tests library."""
from __future__ import annotations

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_coda.tap import TapCoda

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}

TestTapCoda = get_tap_test_class(TapCoda, config=SAMPLE_CONFIG)
