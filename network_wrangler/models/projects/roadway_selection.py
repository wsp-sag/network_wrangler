"""Pydantic Roadway Selection Models which should align with ProjectCard data models."""

from __future__ import annotations

from typing import Annotated, Optional, ClassVar

from pydantic import Field, ConfigDict

from .._base.types import AnyOf, OneOf, ConflictsWith
from .._base.records import RecordModel

from ...params import DEFAULT_SEARCH_MODES
from ...logger import WranglerLogger


class SelectionFormatError(Exception):
    """Raised when there is an issue with the selection format."""

    pass


class SelectNodeDict(RecordModel):
    """Selection of a single roadway node in the `facility` section of a project card."""

    require_one_of: ClassVar[OneOf] = [["osm_node_id", "model_node_id"]]
    initial_selection_fields: ClassVar[list[str]] = ["osm_node_id", "model_node_id"]
    explicit_id_fields: ClassVar[list[str]] = ["osm_node_id", "model_node_id"]
    model_config = ConfigDict(extra="allow")

    osm_node_id: Optional[str] = None
    model_node_id: Optional[int] = None

    _examples = [{"osm_node_id": "12345"}, {"model_node_id": 67890}]

    @property
    def selection_type(self):
        """One of `all` or `explicit_ids`."""
        _explicit_ids = [k for k in self.explicit_id_fields if getattr(self, k)]
        if _explicit_ids:
            return "explicit_ids"
        WranglerLogger.debug(
            f"SelectNode should have an explicit id: {self.explicit_id_fields} \
                Found none in selection dict: \n{self.model_dump(by_alias=True)}"
        )
        raise SelectionFormatError("Select Node should have either `all` or an explicit id.")

    @property
    def explicit_id_selection_dict(self) -> dict:
        """Return a dictionary of field that are explicit ids."""
        return {
            k: [v]
            for k, v in self.model_dump(exclude_none=True, by_alias=True).items()
            if k in self.explicit_id_fields
        }

    @property
    def additional_selection_fields(self) -> list[str]:
        """Return a list of fields that are not part of the initial selection fields."""
        return list(
            set(self.model_dump(exclude_none=True, by_alias=True).keys())
            - set(self.initial_selection_fields)
        )

    @property
    def additional_selection_dict(self) -> dict:
        """Return a dictionary of fields that are not part of the initial selection fields."""
        return {
            k: v
            for k, v in self.model_dump(exclude_none=True, by_alias=True).items()
            if k in self.additional_selection_fields
        }


class SelectNodesDict(RecordModel):
    """Requirements for describing multiple nodes of a project card (e.g. to delete)."""

    require_any_of: ClassVar[AnyOf] = [["osm_node_id", "model_node_id"]]
    _explicit_id_fields: ClassVar[list[str]] = ["osm_node_id", "model_node_id"]
    model_config = ConfigDict(extra="forbid")

    all: Optional[bool] = False
    osm_node_id: Annotated[Optional[list[str]], Field(None, min_length=1)]
    model_node_id: Annotated[Optional[list[int]], Field(min_length=1)]
    ignore_missing: Optional[bool] = True

    _examples = [
        {"osm_node_id": ["12345", "67890"], "model_node_id": [12345, 67890]},
        {"osm_node_id": ["12345", "67890"]},
        {"model_node_id": [12345, 67890]},
    ]

    @property
    def asdict(self) -> dict:
        """Model as a dictionary."""
        return self.model_dump(exclude_none=True, by_alias=True)

    @property
    def fields(self) -> list[str]:
        """List of fields in the selection."""
        return list(self.model_dump(exclude_none=True, by_alias=True).keys())

    @property
    def selection_type(self):
        """One of `all` or `explicit_ids`."""
        if self.all:
            return "all"
        if self.explicit_id_fields:
            return "explicit_ids"
        WranglerLogger.debug(
            f"SelectNodes should have either `all` or an explicit id: {self.explicit_id_fields}. \
                Found neither in nodes selection: \n{self.model_dump(by_alias=True)}"
        )
        raise SelectionFormatError("Select Node should have either `all` or an explicit id.")

    @property
    def explicit_id_fields(self) -> list[str]:
        """Fields which can be used in a selection on their own."""
        return [k for k in self._explicit_id_fields if getattr(self, k)]

    @property
    def explicit_id_selection_dict(self):
        """Return a dictionary of fields that are explicit ids."""
        return {k: v for k, v in self.asdict.items() if k in self.explicit_id_fields}


class SelectLinksDict(RecordModel):
    """requirements for describing links in the `facility` section of a project card.

    Examples:
    ```python
        {'name': ['Main St'], 'modes': ['drive']}
        {'osm_link_id': ['123456789']}
        {'model_link_id': [123456789], 'modes': ['walk']}
        {'all': 'True', 'modes': ['transit']}
    ```

    """

    require_conflicts: ClassVar[ConflictsWith] = [
        ["all", "osm_link_id"],
        ["all", "model_link_id"],
        ["all", "name"],
        ["all", "ref"],
        ["osm_link_id", "model_link_id"],
        ["osm_link_id", "name"],
        ["model_link_id", "name"],
    ]
    require_any_of: ClassVar[AnyOf] = [["name", "ref", "osm_link_id", "model_link_id", "all"]]
    _initial_selection_fields: ClassVar[list[str]] = [
        "name",
        "ref",
        "osm_link_id",
        "model_link_id",
        "all",
    ]
    _explicit_id_fields: ClassVar[list[str]] = ["osm_link_id", "model_link_id"]
    _segment_id_fields: ClassVar[list[str]] = [
        "name",
        "ref",
        "osm_link_id",
        "model_link_id",
        "modes",
    ]
    _special_fields: ClassVar[list[str]] = ["modes", "ignore_missing"]
    model_config = ConfigDict(extra="allow")

    all: Optional[bool] = False
    name: Annotated[Optional[list[str]], Field(None, min_length=1)]
    ref: Annotated[Optional[list[str]], Field(None, min_length=1)]
    osm_link_id: Annotated[Optional[list[str]], Field(None, min_length=1)]
    model_link_id: Annotated[Optional[list[int]], Field(None, min_length=1)]
    modes: list[str] = DEFAULT_SEARCH_MODES
    ignore_missing: Optional[bool] = True

    _examples = [
        {"name": ["Main St"], "modes": ["drive"]},
        {"osm_link_id": ["123456789"]},
        {"model_link_id": [123456789], "modes": ["walk"]},
        {"all": "True", "modes": ["transit"]},
    ]

    @property
    def asdict(self) -> dict:
        """Model as a dictionary."""
        return self.model_dump(exclude_none=True, by_alias=True)

    @property
    def fields(self) -> list[str]:
        """All fields in the selection."""
        return list(self.model_dump(exclude_none=True, by_alias=True).keys())

    @property
    def initial_selection_fields(self) -> list[str]:
        """Fields used in the initial selection of links."""
        if self.all:
            return ["all"]
        return [f for f in self._initial_selection_fields if getattr(self, f)]

    @property
    def explicit_id_fields(self) -> list[str]:
        """Fields that can be used in a slection on their own.

        e.g. `osm_link_id` and `model_link_id`.
        """
        return [k for k in self._explicit_id_fields if getattr(self, k)]

    @property
    def segment_id_fields(self) -> list[str]:
        """Fields that can be used in an intial segment selection.

        e.g. `name`, `ref`, `osm_link_id`, or `model_link_id`.
        """
        return [k for k in self._segment_id_fields if getattr(self, k)]

    @property
    def additional_selection_fields(self):
        """Return a list of fields that are not part of the initial selection fields."""
        _potential = list(
            set(self.fields) - set(self.initial_selection_fields) - set(self._special_fields)
        )
        return [f for f in _potential if getattr(self, f)]

    @property
    def selection_type(self):
        """One of `all`, `explicit_ids`, or `segment`."""
        if self.all:
            return "all"
        if self.explicit_id_fields:
            return "explicit_ids"
        if self.segment_id_fields:
            return "segment"
        else:
            raise SelectionFormatError(
                "If not a segment, Select Links should have either `all` or an explicit id."
            )

    @property
    def explicit_id_selection_dict(self):
        """Return a dictionary of fields that are explicit ids."""
        return {k: v for k, v in self.asdict.items() if k in self.explicit_id_fields}

    @property
    def segment_selection_dict(self):
        """Return a dictionary of fields that are explicit ids."""
        return {k: v for k, v in self.asdict.items() if k in self.segment_id_fields}

    @property
    def additional_selection_dict(self):
        """Return a dictionary of fields that are not part of the initial selection fields."""
        return {k: v for k, v in self.asdict.items() if k in self.additional_selection_fields}


class SelectFacility(RecordModel):
    """Roadway Facility Selection."""

    require_one_of: ClassVar[OneOf] = [["links", "nodes", ["links", "from", "to"]]]
    model_config = ConfigDict(extra="forbid")

    links: Optional[SelectLinksDict] = None
    nodes: Optional[SelectNodesDict] = None
    from_: Annotated[Optional[SelectNodeDict], Field(None, alias="from")]
    to: Optional[SelectNodeDict] = None

    _examples = [
        {
            "links": {"name": ["Main Street"]},
            "from": {"model_node_id": 1},
            "to": {"model_node_id": 2},
        },
        {"nodes": {"osm_node_id": ["1", "2", "3"]}},
        {"nodes": {"model_node_id": [1, 2, 3]}},
        {"links": {"model_link_id": [1, 2, 3]}},
    ]

    @property
    def feature_types(self) -> str:
        """One of `segment`, `links`, or `nodes`."""
        if self.links and self.from_ and self.to:
            return "segment"
        if self.links:
            return "links"
        if self.nodes:
            return "nodes"
        raise ValueError("SelectFacility must have either links or nodes defined.")

    @property
    def selection_type(self) -> str:
        """One of `segment`, `links`, or `nodes`."""
        if self.feature_types == "segment":
            return "segment"
        if self.feature_types == "links":
            return self.links.selection_type
        if self.feature_types == "nodes":
            return self.nodes.selection_type
