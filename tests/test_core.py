"""Tests standard tap features using the built-in SDK tests library."""
from __future__ import annotations

import datetime

from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_coda.tap import TapCoda

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}

TestTapCoda = get_tap_test_class(
    TapCoda,
    config=SAMPLE_CONFIG,
    suite_config=SuiteConfig(
        ignore_no_records=True,
    ),
    # TODO: Enable this test after the SDK handles AllOf properties.  # noqa: TD002, TD003, E501
    include_stream_attribute_tests=False,
)
