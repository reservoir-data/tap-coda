"""Stream type classes for tap-coda."""

from __future__ import annotations

import typing as t

from tap_coda.client import CodaStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class Docs(CodaStream):
    """Coda documents."""

    name = "docs"
    path = "/docs"
    openapi_ref = "Doc"

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Initialize the `docs` stream."""
        super().__init__(*args, **kwargs)
        self.schema["properties"]["sourceDoc"] = {
            "x-schema-name": "DocReference",
            "description": "Reference to a Coda doc.",
            "type": "object",
            "required": [
                "id",
                "type",
                "browserLink",
                "href",
            ],
            "additionalProperties": False,
            "properties": {
                "id": {
                    "type": "string",
                    "description": "ID of the Coda doc.",
                    "example": "AbCDeFGH",
                },
                "type": {
                    "type": "string",
                    "description": "The type of this resource.",
                    "enum": [
                        "doc",
                    ],
                    "x-tsType": "Type.Doc",
                },
                "href": {
                    "type": "string",
                    "format": "url",
                    "description": "API link to the Coda doc.",
                    "example": "https://coda.io/apis/v1/docs/AbCDeFGH",
                },
                "browserLink": {
                    "type": "string",
                    "format": "url",
                    "description": "Browser-friendly link to the Coda doc.",
                    "example": "https://coda.io/d/_dAbCDeFGH",
                },
            },
        }

    def get_child_context(
        self,
        record: dict,
        context: Context | None,  # noqa: ARG002
    ) -> dict:
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

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
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

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Initialize `formulas` stream."""
        super().__init__(*args, **kwargs)
        del self.schema["properties"]["value"]
        self.schema["properties"]["value__string"] = {
            "description": "A Coda result or entity expressed as a primitive type.",
            "type": "string",
            "example": "$12.34",
        }
        self.schema["properties"]["value__number"] = {
            "description": "A Coda result or entity expressed as a primitive type.",
            "type": "number",
            "example": 12.34,
        }
        self.schema["properties"]["value__boolean"] = {
            "description": "A Coda result or entity expressed as a primitive type.",
            "type": "boolean",
            "example": True,
        }

    def post_process(
        self,
        row: dict,
        context: Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """Post-process formula records.

        Args:
            row: A formula record.
            context: The stream context.

        Returns:
            A post-processed formula record.
        """
        value = row.pop("value", None)
        if isinstance(value, str):
            row["value__string"] = value
        elif isinstance(value, bool):
            row["value__boolean"] = value
        elif isinstance(value, float):
            row["value__number"] = value
        return row


class Permissions(_DocChild):
    """Coda document permissions."""

    name = "permissions"
    path = "/docs/{docId}/acl/permissions"
    openapi_ref = "Permission"

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
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

    def get_child_context(self, record: dict, context: Context | None) -> dict:
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


class _TableChild(CodaStream):
    parent_stream_type = Tables

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
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

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
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

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Initialize a stream with `docs` as parent stream.

        Args:
            args: Positional arguments.
            kwargs: Keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self._schema["properties"]["values"].pop("additionalProperties")
