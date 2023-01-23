"""Stream type classes for tap-coda."""

from __future__ import annotations

from typing import Any

from tap_coda.client import CodaStream


class Docs(CodaStream):
    """Coda documents."""

    name = "docs"
    path = "/docs"
    openapi_ref = "Doc"

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Get context for docs child streams.

        Args:
            record: A `docs` record.
            context: The stream context.

        Returns:
            Child stream context dictionary.
        """
        return {"docId": record["id"]}


class _DocChild(CodaStream):
    parent_stream_type = Docs

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize a stream with `docs` as parent stream.

        Args:
            args: Positional arguments.
            kwargs: Keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self._schema["properties"]["docId"] = {
            "type": "string",
            "description": "Parent document ID",
        }


class Pages(_DocChild):
    """Coda document pages."""

    name = "pages"
    path = "/docs/{docId}/pages"
    openapi_ref = "Page"


class Controls(_DocChild):
    """Coda document controls."""

    name = "controls"
    path = "/docs/{docId}/controls"
    openapi_ref = "ControlReference"


class Formulas(_DocChild):
    """Coda document pages."""

    name = "formulas"
    path = "/docs/{docId}/formulas"
    openapi_ref = "Formula"


class Permissions(_DocChild):
    """Coda document permissions."""

    name = "permissions"
    path = "/docs/{docId}/acl/permissions"
    openapi_ref = "Permission"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize a stream with `docs` as parent stream.

        Args:
            args: Positional arguments.
            kwargs: Keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self._schema["properties"]["principal"]["type"] = "object"


class Tables(_DocChild):
    """Coda document tables."""

    name = "tables"
    path = "/docs/{docId}/tables"
    openapi_ref = "Table"

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Get context for tables child streams.

        Args:
            record: A `tables` record.
            context: The stream context.

        Returns:
            Child stream context dictionary.
        """
        return {
            "docId": context["docId"] if context else None,
            "tableIdOrName": record["id"],
        }


class _TableChild(_DocChild):
    parent_stream_type = Tables

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize a stream with `tables` as parent stream.

        Args:
            args: Positional arguments.
            kwargs: Keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self._schema["properties"]["tableIdOrName"] = {
            "type": "string",
            "description": "Parent table ID",
        }


class Columns(_TableChild):
    """Coda document table columns."""

    name = "columns"
    path = "/docs/{docId}/tables/{tableIdOrName}/columns"
    parent_stream_type = Tables
    openapi_ref = "Column"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize a stream with `docs` as parent stream.

        Args:
            args: Positional arguments.
            kwargs: Keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self._schema["properties"]["format"]["type"] = "object"


class Rows(_TableChild):
    """Coda document table rows."""

    name = "rows"
    path = "/docs/{docId}/tables/{tableIdOrName}/rows"
    parent_stream_type = Tables
    openapi_ref = "Row"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize a stream with `docs` as parent stream.

        Args:
            args: Positional arguments.
            kwargs: Keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self._schema["properties"]["values"].pop("additionalProperties")
