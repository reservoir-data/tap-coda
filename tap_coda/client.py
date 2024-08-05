"""REST client handling, including CodaStream base class."""

from __future__ import annotations

import typing as t

from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class CodaStream(RESTStream):
    """Coda stream class."""

    openapi_ref: str

    url_base = "https://coda.io/apis/v1"
    records_jsonpath = "$.items[*]"
    next_page_token_jsonpath = "$.nextPageToken"  # noqa: S105
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator for the Coda API.
        """
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config["auth_token"],
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: str | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict[str, t.Any] = {
            "limit": 100,
        }
        if next_page_token:
            params["pageToken"] = next_page_token
        return params
