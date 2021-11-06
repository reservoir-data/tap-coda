"""REST client handling, including CodaStream base class."""

from pathlib import Path
from typing import Any, Dict, Optional

from jsonschema.validators import RefResolver
from singer.transform import _resolve_schema_references
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.streams import RESTStream
from toolz.dicttoolz import get_in

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class CodaStream(RESTStream):
    """Coda stream class."""

    url_base = "https://coda.io/apis/v1"
    records_jsonpath = "$.items[*]"
    next_page_token_jsonpath = "$.nextPageToken"
    primary_keys = ["id"]
    replication_key = None

    @classmethod
    def get_schema(cls, openapi: dict, resolver: RefResolver):
        """Get schema from OpenAPI object and JSONSchema $ref resolver."""
        schema = get_in(
            [
                "paths",
                cls.path,
                cls.rest_method.lower(),
                "responses",
                "200",
                "content",
                "application/json",
                "schema",
            ],
            openapi,
        )
        resolved = _resolve_schema_references(schema, resolver)
        return resolved["properties"]["items"]["items"]

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object."""
        return BearerTokenAuthenticator.create_for_stream(
            self, token=self.config.get("auth_token")
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[str]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {
            "limit": 100,
        }
        if next_page_token:
            params["pageToken"] = next_page_token
        return params
