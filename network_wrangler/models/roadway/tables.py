"""Datamodels for Roadway Network Tables.

This module contains the datamodels used to validate the format and types of
Roadway Network tables.

Includes:
- RoadLinksTable
- RoadNodesTable
- RoadShapesTable
- ExplodedScopedLinkPropertyTable
"""

from __future__ import annotations

import datetime as dt
from typing import Any, ClassVar, Optional

import numpy as np
import pandas as pd
import pandera as pa
from pandas import Int64Dtype as Int64
from pandera import DataFrameModel
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries

from ...logger import WranglerLogger
from .._base.db import TableForeignKeys, TablePrimaryKeys
from .._base.tables import validate_pyd
from .types import ScopedLinkValueList

RoadLinksAttrs = {
    "name": "road_links",
    "primary_key": "model_link_id",
    "source_file": None,
    "display_cols": ["model_link_id", "osm_link_id", "name"],
    "explicit_ids": ["model_link_id", "osm_link_id"],
    "geometry_props": ["geometry"],
    "idx_col": "model_link_id_idx",
}


class RoadLinksTable(DataFrameModel):
    """Datamodel used to validate if links_df is of correct format and types."""

    model_link_id: Series[int] = pa.Field(coerce=True, unique=True)
    model_link_id_idx: Optional[Series[int]] = pa.Field(coerce=True, unique=True)
    A: Series[int] = pa.Field(nullable=False, coerce=True)
    B: Series[int] = pa.Field(nullable=False, coerce=True)
    geometry: GeoSeries = pa.Field(nullable=False)
    name: Series[str] = pa.Field(nullable=False, default="unknown")
    rail_only: Series[bool] = pa.Field(coerce=True, nullable=False, default=False)
    bus_only: Series[bool] = pa.Field(coerce=True, nullable=False, default=False)
    drive_access: Series[bool] = pa.Field(coerce=True, nullable=False, default=True)
    bike_access: Series[bool] = pa.Field(coerce=True, nullable=False, default=True)
    walk_access: Series[bool] = pa.Field(coerce=True, nullable=False, default=True)
    distance: Series[float] = pa.Field(coerce=True, nullable=False)

    roadway: Series[str] = pa.Field(nullable=False, default="road")
    projects: Series[str] = pa.Field(coerce=True, default="")
    managed: Series[int] = pa.Field(coerce=True, nullable=False, default=0)

    shape_id: Series[str] = pa.Field(coerce=True, nullable=True)
    lanes: Series[int] = pa.Field(coerce=True, nullable=False)
    price: Series[float] = pa.Field(coerce=True, nullable=False, default=0)

    # Optional Fields

    access: Optional[Series[Any]] = pa.Field(coerce=True, nullable=True, default=None)

    sc_lanes: Optional[Series[object]] = pa.Field(coerce=True, nullable=True, default=None)
    sc_price: Optional[Series[object]] = pa.Field(coerce=True, nullable=True, default=None)

    ML_projects: Series[str] = pa.Field(coerce=True, default="")
    ML_lanes: Optional[Series[Int64]] = pa.Field(coerce=True, nullable=True, default=None)
    ML_price: Optional[Series[float]] = pa.Field(coerce=True, nullable=True, default=0)
    ML_access: Optional[Series[Any]] = pa.Field(coerce=True, nullable=True, default=True)
    ML_access_point: Optional[Series[bool]] = pa.Field(
        coerce=True,
        default=False,
    )
    ML_egress_point: Optional[Series[bool]] = pa.Field(
        coerce=True,
        default=False,
    )
    sc_ML_lanes: Optional[Series[object]] = pa.Field(
        coerce=True,
        nullable=True,
        default=None,
    )
    sc_ML_price: Optional[Series[object]] = pa.Field(
        coerce=True,
        nullable=True,
        default=None,
    )
    sc_ML_access: Optional[Series[object]] = pa.Field(
        coerce=True,
        nullable=True,
        default=None,
    )

    ML_geometry: Optional[GeoSeries] = pa.Field(nullable=True, coerce=True, default=None)
    ML_shape_id: Optional[Series[str]] = pa.Field(nullable=True, coerce=True, default=None)

    truck_access: Optional[Series[bool]] = pa.Field(coerce=True, nullable=True, default=True)
    osm_link_id: Series[str] = pa.Field(coerce=True, nullable=True, default="")
    # todo this should be List[dict] but ranch output something else so had to have it be Any.
    locationReferences: Optional[Series[Any]] = pa.Field(
        coerce=True,
        nullable=True,
        default="",
    )

    GP_A: Optional[Series[Int64]] = pa.Field(coerce=True, nullable=True, default=None)
    GP_B: Optional[Series[Int64]] = pa.Field(coerce=True, nullable=True, default=None)

    class Config:
        """Config for RoadLinksTable."""

        add_missing_columns = True
        coerce = True
        unique: ClassVar[list[str]] = ["A", "B"]

    @pa.check("sc_*", regex=True, element_wise=True)
    def check_scoped_fields(cls, scoped_value: Series) -> Series[bool]:
        """Checks that all fields starting with 'sc_' or 'sc_ML_' are valid ScopedLinkValueList.

        Custom check to validate fields starting with 'sc_' or 'sc_ML_'
        against a ScopedLinkValueItem model, handling both mandatory and optional fields.
        """
        if scoped_value is None or (not isinstance(scoped_value, list) and pd.isna(scoped_value)):
            return True
        return validate_pyd(scoped_value, ScopedLinkValueList)


RoadNodesAttrs = {
    "name": "road_nodes",
    "primary_key": "model_node_id",
    "source_file": None,
    "display_cols": ["model_node_id", "osm_node_id", "X", "Y"],
    "explicit_ids": ["model_node_id", "osm_node_id"],
    "geometry_props": ["X", "Y", "geometry"],
    "idx_col": "model_node_id_idx",
}


class RoadNodesTable(DataFrameModel):
    """Datamodel used to validate if links_df is of correct format and types."""

    model_node_id: Series[int] = pa.Field(coerce=True, unique=True, nullable=False)
    model_node_idx: Optional[Series[int]] = pa.Field(coerce=True, unique=True, nullable=False)
    geometry: GeoSeries
    X: Series[float] = pa.Field(coerce=True, nullable=False)
    Y: Series[float] = pa.Field(coerce=True, nullable=False)

    # optional fields
    osm_node_id: Series[str] = pa.Field(
        coerce=True,
        nullable=True,
        default="",
    )
    projects: Series[str] = pa.Field(coerce=True, default="")
    inboundReferenceIds: Optional[Series[list[str]]] = pa.Field(coerce=True, nullable=True)
    outboundReferenceIds: Optional[Series[list[str]]] = pa.Field(coerce=True, nullable=True)

    class Config:
        """Config for RoadNodesTable."""

        add_missing_columns = True
        coerce = True
        _pk: ClassVar[TablePrimaryKeys] = ["model_node_id"]


RoadShapesAttrs = {
    "name": "road_shapes",
    "primary_key": "shape_id",
    "source_file": None,
    "display_cols": ["shape_id"],
    "explicit_ids": ["shape_id"],
    "geometry_props": ["geometry"],
    "idx_col": "shape_id_idx",
}


class RoadShapesTable(DataFrameModel):
    """Datamodel used to validate if links_df is of correct format and types."""

    shape_id: Series[str] = pa.Field(unique=True)
    shape_id_idx: Optional[Series[int]] = pa.Field(unique=True)

    geometry: GeoSeries = pa.Field()
    ref_shape_id: Optional[Series] = pa.Field(nullable=True)

    class Config:
        """Config for RoadShapesTable."""

        coerce = True
        _pk: ClassVar[TablePrimaryKeys] = ["shape_id"]


class ExplodedScopedLinkPropertyTable(DataFrameModel):
    """Datamodel used to validate an exploded links_df by scope."""

    model_link_id: Series[int]
    category: Series[Any]
    timespan: Series[list[str]]
    start_time: Series[dt.datetime]
    end_time: Series[dt.datetime]
    scoped: Series[Any] = pa.Field(default=None, nullable=True)

    class Config:
        """Config for ExplodedScopedLinkPropertySchema."""

        name = "ExplodedScopedLinkPropertySchema"
        coerce = True
