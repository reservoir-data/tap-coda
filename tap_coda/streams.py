"""Stream type classes for tap-coda."""

from typing import Optional

from tap_coda.client import CodaStream


class Docs(CodaStream):
    "Coda documents."

    name = "docs"
    path = "/docs"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"docId": record["id"]}


class _DocChild(CodaStream):
    parent_stream_type = Docs

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._schema["properties"]["docId"] = {
            "type": "string",
            "description": "Parent document ID",
        }


class Pages(_DocChild):
    "Coda document pages."

    name = "pages"
    path = "/docs/{docId}/pages"


class Controls(_DocChild):
    "Coda document controls."

    name = "controls"
    path = "/docs/{docId}/controls"


class Formulas(_DocChild):
    "Coda document pages."

    name = "formulas"
    path = "/docs/{docId}/formulas"


class Permissions(_DocChild):
    "Coda document permissions."

    name = "permissions"
    path = "/docs/{docId}/acl/permissions"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._schema["properties"]["principal"]["type"] = "object"


class Tables(_DocChild):
    "Coda document tables."

    name = "tables"
    path = "/docs/{docId}/tables"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"docId": context["docId"], "tableIdOrName": record["id"]}


class _TableChild(_DocChild):
    parent_stream_type = Tables

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._schema["properties"]["tableIdOrName"] = {
            "type": "string",
            "description": "Parent table ID",
        }


class Columns(_TableChild):
    "Coda document table columns."

    name = "columns"
    path = "/docs/{docId}/tables/{tableIdOrName}/columns"
    parent_stream_type = Tables

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._schema["properties"]["format"]["type"] = "object"


class Rows(_TableChild):
    "Coda document table rows."

    name = "rows"
    path = "/docs/{docId}/tables/{tableIdOrName}/rows"
    parent_stream_type = Tables

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._schema["properties"]["values"].pop("additionalProperties")
