"""Pytest configuration for tests in this directory."""  # noqa: CPY001

from __future__ import annotations

import fnmatch

import pytest

pytest_plugins = "pytester"

XFAIL_SCHEMA_MISMATCH = pytest.mark.xfail(reason="Schema mismatch against OpenAPI spec")
SCHEMA_MISMATCH: set[str] = {
    "columns",
    "rows",
    "tables",
}


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Skip tests that require a live API key."""
    test_name = item.name.split("::")[-1]

    if any(fnmatch.fnmatch(test_name, pattern) for pattern in SCHEMA_MISMATCH):
        item.add_marker(XFAIL_SCHEMA_MISMATCH)
