"""REST client handling, including CodaStream base class."""

from __future__ import annotations

import sys
import typing as t

from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class CodaStream(RESTStream[str]):
    """Coda stream class."""

    openapi_ref: str

    url_base = "https://coda.io/apis/v1"
    records_jsonpath = "$.items[*]"
    next_page_token_jsonpath = "$.nextPageToken"  # noqa: S105
    primary_keys = ("id",)
    replication_key = None

    @property
    @override
    def authenticator(self) -> BearerTokenAuthenticator:
        return BearerTokenAuthenticator(token=self.config["auth_token"])

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: str | None,
    ) -> dict[str, t.Any]:
        """Get URL parameters for the Coda API."""
        params: dict[str, t.Any] = {
            "limit": 100,
        }
        if next_page_token:
            params["pageToken"] = next_page_token
        return params
