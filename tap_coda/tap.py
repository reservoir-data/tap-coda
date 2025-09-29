"""Coda tap class."""

from __future__ import annotations

from typing import TYPE_CHECKING, override

import requests
from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.singerlib import resolve_schema_references

from tap_coda import streams

if TYPE_CHECKING:
    from tap_coda.client import CodaStream

STREAM_TYPES: list[type[CodaStream]] = [
    streams.Docs,
    streams.Pages,
    streams.Formulas,
    streams.Controls,
    streams.Permissions,
    streams.Tables,
    streams.Columns,
    streams.Rows,
]


class TapCoda(Tap):
    """Singer tap for Coda, built with the Meltano SDK for Singer Taps."""

    name = "tap-coda"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,
            description="The token to authenticate against the API service.",
        ),
    ).to_dict()

    @staticmethod
    def get_openapi() -> dict:
        """Retrieve the OpenAPI spec for the API.

        Returns:
            The OpenAPI spec dictionary.
        """
        response = requests.get("https://coda.io/apis/v1/openapi.json", timeout=5)
        response.raise_for_status()
        return response.json()

    @override
    def discover_streams(self) -> list[CodaStream]:
        streams = []
        openapi_schema = self.get_openapi()

        for stream_class in STREAM_TYPES:
            schema = {
                "$ref": f"#/components/schemas/{stream_class.openapi_ref}",
                "components": openapi_schema["components"],
            }
            resolved_schema = resolve_schema_references(schema)
            streams.append(stream_class(tap=self, schema=resolved_schema))
        return streams
