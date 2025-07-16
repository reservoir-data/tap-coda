"""Coda tap class."""

from __future__ import annotations

import requests
from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.singerlib import resolve_schema_references

from tap_coda.streams import (
    CodaStream,
    Columns,
    Controls,
    Docs,
    Formulas,
    Pages,
    Permissions,
    Rows,
    Tables,
)

STREAM_TYPES: list[type[CodaStream]] = [
    Docs,
    Pages,
    Formulas,
    Controls,
    Permissions,
    Tables,
    Columns,
    Rows,
]


class TapCoda(Tap):
    """Singer tap for Coda, built with the Meltano SDK for Singer Taps."""

    name = "tap-coda"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
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

    def discover_streams(self) -> list[CodaStream]:
        """Return a list of discovered streams.

        Returns:
            A list of streams.
        """
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
