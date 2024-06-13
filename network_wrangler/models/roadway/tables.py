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

from typing import Any, Optional
import datetime as dt

import pandas as pd
import pandera as pa

from pandas import Int64Dtype as Int64
from pandera import DataFrameModel
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries

from .._base.tables import validate_pyd
from .types import ScopedLinkValueList


class RoadLinksTable(DataFrameModel):
    """Datamodel used to validate if links_df is of correct format and types."""

    model_link_id: Series[int] = pa.Field(coerce=True, unique=True)
    model_link_id_idx: Optional[Series[int]] = pa.Field(coerce=True, unique=True)
    A: Series[int] = pa.Field(nullable=False, coerce=True)
    B: Series[int] = pa.Field(nullable=False, coerce=True)
    geometry: GeoSeries = pa.Field(nullable=False)
    name: Series[str] = pa.Field(nullable=False)
    rail_only: Series[bool] = pa.Field(coerce=True, nullable=False, default=False)
    bus_only: Series[bool] = pa.Field(coerce=True, nullable=False, default=False)
    drive_access: Series[bool] = pa.Field(coerce=True, nullable=False, default=True)
    bike_access: Series[bool] = pa.Field(coerce=True, nullable=False, default=True)
    walk_access: Series[bool] = pa.Field(coerce=True, nullable=False, default=True)
    distance: Series[float] = pa.Field(coerce=True, nullable=False)

    roadway: Series[str] = pa.Field(nullable=False, default="road")
    managed: Series[int] = pa.Field(coerce=True, nullable=False, default=0)

    shape_id: Series[str] = pa.Field(coerce=True, nullable=True)
    lanes: Series[int] = pa.Field(coerce=True, nullable=False)
    price: Series[float] = pa.Field(coerce=True, nullable=False, default=0)

    # Optional Fields

    access: Optional[Series[Any]] = pa.Field(coerce=True, nullable=True, default=True)

    sc_lanes: Optional[Series[object]] = pa.Field(coerce=True, nullable=True, default=None)
    sc_price: Optional[Series[object]] = pa.Field(coerce=True, nullable=True, default=None)

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

        name = "RoadLinksTable"
        add_missing_columns = True
        coerce = True

    @pa.dataframe_check
    def unique_ab(cls, df: pd.DataFrame) -> bool:
        """Check that combination of A and B are unique."""
        return ~df[["A", "B"]].duplicated()

    # TODO add check that if there is managed>1 anywhere, that ML_ columns are present.

    @pa.dataframe_check
    def check_scoped_fields(cls, df: pd.DataFrame) -> Series[bool]:
        """Checks that all fields starting with 'sc_' or 'sc_ML_' are valid ScopedLinkValueList.

        Custom check to validate fields starting with 'sc_' or 'sc_ML_'
        against a ScopedLinkValueItem model, handling both mandatory and optional fields.
        """
        scoped_fields = [
            col for col in df.columns if col.startswith("sc_") or col.startswith("sc_ML")
        ]
        results = []

        for field in scoped_fields:
            if df[field].notna().any():
                results.append(
                    df[field].dropna().apply(validate_pyd, args=(ScopedLinkValueList,)).all()
                )
            else:
                # Handling optional fields: Assume validation is true if the field is entirely NA
                results.append(True)

        # Combine all results: True if all fields pass validation
        return pd.Series(all(results), index=df.index)


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

    inboundReferenceIds: Optional[Series[list[str]]] = pa.Field(coerce=True, nullable=True)
    outboundReferenceIds: Optional[Series[list[str]]] = pa.Field(coerce=True, nullable=True)

    class Config:
        """Config for RoadNodesTable."""

        name = "RoadNodesTable"
        add_missing_columns = True
        coerce = True


class RoadShapesTable(DataFrameModel):
    """Datamodel used to validate if links_df is of correct format and types."""

    shape_id: Series[str] = pa.Field(unique=True)
    shape_id_idx: Optional[Series[int]] = pa.Field(unique=True)

    geometry: GeoSeries = pa.Field()
    ref_shape_id: Optional[Series] = pa.Field(nullable=True)

    class Config:
        """Config for RoadShapesTable."""

        name = "ShapesSchema"
        coerce = True


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
