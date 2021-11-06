"""Coda tap class."""

from typing import List, Type

import requests
from jsonschema.validators import RefResolver
from singer_sdk import Tap
from singer_sdk import typing as th

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

STREAM_TYPES: List[Type[CodaStream]] = [
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
    """Coda tap class."""

    name = "tap-coda"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service.",
        ),
    ).to_dict()

    def __init__(self, *args, **kwargs) -> None:
        self.openapi = self.get_openapi()
        self.resolver = RefResolver("", self.openapi)
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_openapi() -> dict:
        response = requests.get("https://coda.io/apis/v1/openapi.json")
        response.raise_for_status()
        return response.json()

    def discover_streams(self) -> List[CodaStream]:
        """Return a list of discovered streams."""
        streams = []
        for stream_class in STREAM_TYPES:
            stream = stream_class(
                tap=self,
                schema=stream_class.get_schema(self.openapi, self.resolver),
            )
            streams.append(stream)
        return streams
