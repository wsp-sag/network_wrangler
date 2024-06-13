"""Functions to create RoadShapesTable from various data."""

from typing import Union

import pandas as pd
import geopandas as gpd

from pandera.typing import DataFrame

from ...models.roadway.tables import RoadShapesTable
from ...models._base.validate import validate_df_to_model
from ...params import ShapesParams, LAT_LON_CRS, ROAD_SHAPE_ID_SCALAR
from ...utils.data import attach_parameters_to_df, coerce_gdf
from ...utils.utils import generate_list_of_new_ids
from ...utils.geo import _offset_geometry_meters
from ...logger import WranglerLogger
from ..utils import set_df_index_to_pk


def df_to_shapes_df(
    shapes_df: gpd.GeoDataFrame,
    in_crs: int = LAT_LON_CRS,
    shapes_params: Union[None, ShapesParams] = None,
) -> DataFrame[RoadShapesTable]:
    """Sets index to be a copy of the primary key, validates to RoadShapesTable and aligns CRS.

    Args:
        shapes_df (gpd.GeoDataFrame): _description_
        in_crs: coordinate reference system number of incoming df. ONLY used if shapes_df is not
            already set. Defaults to LAT_LON_CRS.
        shapes_params: a ShapesParams instance. Defaults to a default ShapesParams instance.

    Returns:
        DataFrame[RoadShapesTable]
    """
    WranglerLogger.debug(f"Creating {len(shapes_df)} shapes.")
    if not isinstance(shapes_df, gpd.GeoDataFrame):
        shapes_df = coerce_gdf(shapes_df, in_crs=in_crs)

    if shapes_df.crs != LAT_LON_CRS:
        shapes_df = shapes_df.to_crs(LAT_LON_CRS)

    shapes_params = ShapesParams() if shapes_params is None else shapes_params
    shapes_df = attach_parameters_to_df(shapes_df, shapes_params)
    shapes_df = set_df_index_to_pk(shapes_df)

    shapes_df.gdf_name = "network_shapes"
    shapes_df = validate_df_to_model(shapes_df, RoadShapesTable)
    assert "params" in shapes_df.__dict__

    return shapes_df


def create_offset_shapes(
    shapes_df: DataFrame[RoadShapesTable],
    shape_ids: list,
    offset_dist_meters: float = 10,
    id_scalar: int = ROAD_SHAPE_ID_SCALAR,
) -> DataFrame[RoadShapesTable]:
    """Create a RoadShapesTable of new shape records for shape_ids which are offset.

    Args:
        shapes_df (RoadShapesTable): Original RoadShapesTable to add on to.
        shape_ids (list): Shape_ids to create offsets for.
        offset_dist_meters (float, optional): Distance in meters to offset by. Defaults to 10.
        id_scalar (int, optional): Increment to add to shape_id. Defaults to SHAPE_ID_SCALAR.

    Returns:
      RoadShapesTable: of offset shapes and a column `ref_shape_id` which references
            the shape_id which was offset to create it.
    """
    offset_shapes_df = pd.DataFrame(
        {
            "shape_id": generate_list_of_new_ids(
                shape_ids, shapes_df.shape_ids.to_list, id_scalar
            ),
            "ref_shape_id": shape_ids,
        }
    )

    ref_shapes_df = shapes_df[shapes_df["shape_id"].isin(shape_ids)].copy()

    ref_shapes_df["offset_shape_id"] = generate_list_of_new_ids(
        ref_shapes_df.shape_id.to_list, shapes_df.shape_ids.to_list, id_scalar
    )

    ref_shapes_df["geometry"] = _offset_geometry_meters(ref_shapes_df.geometry, offset_dist_meters)

    offset_shapes_df = ref_shapes_df.rename(
        columns={
            "shape_id": "ref_shape_id",
            "offset_shape_id": "shape_id",
        }
    )

    offset_shapes_gdf = gpd.GeoDataFrame(offset_shapes_df, geometry="geometry", crs=shapes_df.crs)

    offset_shapes_gdf = RoadShapesTable.validate(offset_shapes_gdf, lazy=True)

    return offset_shapes_gdf


def add_offset_shapes(
    shapes_df: DataFrame[RoadShapesTable],
    shape_ids: list,
    offset_dist_meters: float = 10,
    id_scalar: int = ROAD_SHAPE_ID_SCALAR,
) -> DataFrame[RoadShapesTable]:
    """Appends a RoadShapesTable with new shape records for shape_ids which are offset from orig.

    Args:
        shapes_df (RoadShapesTable): Original RoadShapesTable to add on to.
        shape_ids (list): Shape_ids to create offsets for.
        offset_dist_meters (float, optional): Distance in meters to offset by. Defaults to 10.
        id_scalar (int, optional): Increment to add to shape_id. Defaults to SHAPE_ID_SCALAR.

    Returns:
        RoadShapesTable: with added offset shape_ids and a column `ref_shape_id` which references
            the shape_id which was offset to create it.
    """
    offset_shapes_df = create_offset_shapes(shapes_df, shape_ids, offset_dist_meters, id_scalar)
    shapes_df = pd.concat([shapes_df, offset_shapes_df])
    shapes_df = RoadShapesTable.validate(shapes_df, lazy=True)
    return shapes_df
