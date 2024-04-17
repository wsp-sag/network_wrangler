import time

from dataclasses import dataclass, field
from pathlib import Path
from typing import Union, Any, Optional, List, Literal

import geopandas as gpd
import numpy as np
import pandas as pd
import pandera as pa

from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries
from pandera import check_input, check_output

from ..logger import WranglerLogger

from .shapes import ShapesSchema
from .utils import create_unique_shape_id
from ..utils import (
    coerce_val_to_series_type,
    length_of_linestring_miles,
    linestring_from_nodes,
    read_table,
    write_table,
    fk_in_pk,
)
from ..utils.time import parse_timespans_to_secs


MODES_TO_NETWORK_LINK_VARIABLES = {
    "drive": ["drive_access"],
    "bus": ["bus_only", "drive_access"],
    "rail": ["rail_only"],
    "transit": ["bus_only", "rail_only", "drive_access"],
    "walk": ["walk_access"],
    "bike": ["bike_access"],
}


@dataclass
class LinksParams:
    primary_key: str = field(default="model_link_id")
    _addtl_unique_ids: list[str] = field(default_factory=lambda: [])
    _addtl_explicit_ids: list[str] = field(default_factory=lambda: ["osm_link_id"])
    from_node: str = field(default="A")
    to_node: str = field(default="B")
    fk_to_shape: str = field(default="shape_id")
    table_type: Literal["links"] = field(default="links")
    source_file: str = field(default=None)
    modes_to_network_link_variables: dict = field(
        default_factory=lambda: MODES_TO_NETWORK_LINK_VARIABLES
    )

    @property
    def idx_col(self):
        return self.primary_key + "_idx"

    @property
    def fks_to_nodes(self):
        return [self.from_node, self.to_node]

    @property
    def unique_ids(self):
        _uids = self._addtl_unique_ids + [self.primary_key]
        return list(set(_uids))

    @property
    def explicit_ids(self):
        return list(set(self.unique_ids + self._addtl_explicit_ids))

    @property
    def display_cols(self):
        _addtl = ["lanes"]
        return list(set(self.explicit_ids + self.fks_to_nodes + _addtl))


class LinksSchema(pa.DataFrameModel):
    """Datamodel used to validate if links_df is of correct format and types."""

    model_link_id: Series[int] = pa.Field(coerce=True, unique=True)
    A: Series[Any] = pa.Field(nullable=False)
    B: Series[Any] = pa.Field(nullable=False)
    geometry: GeoSeries = pa.Field(nullable=False)
    name: Series[str] = pa.Field(nullable=False)
    rail_only: Series[bool] = pa.Field(coerce=True, nullable=False, default=False)
    bus_only: Series[bool] = pa.Field(coerce=True, nullable=False, default=False)
    drive_access: Series[bool] = pa.Field(coerce=True, nullable=False, default=True)
    bike_access: Series[bool] = pa.Field(coerce=True, nullable=False, default=True)
    walk_access: Series[bool] = pa.Field(coerce=True, nullable=False, default=True)

    roadway: Series[str] = pa.Field(nullable=False)
    lanes: Series[int] = pa.Field(coerce=True, nullable=False)

    # Optional Fields
    truck_access: Optional[Series[bool]] = pa.Field(
        coerce=True, nullable=True, default=True
    )
    osm_link_id: Optional[Series[str]] = pa.Field(
        coerce=True, nullable=True, default=""
    )
    locationReferences: Optional[Series[List[dict]]] = pa.Field(
        coerce=True,
        nullable=True,
        default="",
    )
    shape_id: Optional[Series[str]] = pa.Field(nullable=True, default="", coerce=True)

    class Config:
        name = "LinkSchema"
        add_missing_columns = True
        coerce = True

    @pa.dataframe_check
    def unique_ab(cls, df: pd.DataFrame) -> bool:
        return ~df[["A", "B"]].duplicated()


@check_output(LinksSchema, inplace=True)
def read_links(
    filename: Union[Path, str],
    crs: int = 4326,
    links_params: Union[dict, LinksParams] = None,
    nodes_df: gpd.GeoDataFrame = None,
) -> gpd.GeoDataFrame:
    """Reads links and returns a geodataframe of links.

    Sets index to be a copy of the primary key.
    Validates output dataframe using LinksSchema.

    Args:
        filename (str): file to read links in from.
        crs: coordinate reference system number any link geometries are stored in.
            Defaults to 4323.
        link_params: a LinkParams instance. Defaults to a default LinkParams instance.
    """
    WranglerLogger.debug(f"Reading links from {filename}.")
    start_time = time.time()
    links_df = read_table(filename)
    WranglerLogger.debug(
        f"Read {len(links_df)} links from file in {round(time.time() - start_time,2)}."
    )
    links_df = _links_data_to_links_df(
        links_df, links_crs=crs, links_params=links_params, nodes_df=nodes_df
    )
    links_df.params.source_file = filename
    WranglerLogger.info(
        f"Read + transformed {len(links_df)} links from \
            {filename} in {round(time.time() - start_time,2)}."
    )
    return links_df


def validate_links_have_nodes(links_df: pd.DataFrame, nodes_df: pd.DataFrame) -> bool:
    """Checks if links have nodes and returns a boolean.

    raises: ValueError if nodes_df is missing and A or B node
    """
    nodes_in_links = list(set(links_df["A"]).union(set(links_df["B"])))

    fk_valid, fk_missing = fk_in_pk(nodes_df.index, nodes_in_links)
    if not fk_valid:
        WranglerLogger.error(f"Nodes missing from links: {fk_missing}")
        raise ValueError(f"Links are missing these nodes: {fk_missing}")
    return True


def _add_link_geometries_from_nodes(
    links_df: pd.DataFrame, nodes_df: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Create location references and link geometries from nodes.

    If already has either, then will just fill NA.
    """
    WranglerLogger.debug("Creating link geometries from nodes")
    geo_start_time = time.time()

    if links_df is gpd.GeoDataFrame:
        links_gdf = links_df
        links_gdf[links_df.geometry.isnull()] = linestring_from_nodes(
            links_gdf.geometry.isnull(), nodes_df
        )
    else:
        geometry = linestring_from_nodes(links_df, nodes_df)
        # WranglerLogger.debug(f"----Geometry:\n{geometry}")
        links_gdf = gpd.GeoDataFrame(links_df, geometry=geometry, crs=nodes_df.crs)
        links_gdf.__dict__["params"] = links_df.params
        # WranglerLogger.debug(f"----LINKs:\n{links_gdf[['A','B','geometry']]}")
    WranglerLogger.debug(
        f"Created link geometries from nodes in {round(time.time() - geo_start_time,2)}."
    )
    # WranglerLogger.debug(f"----Links:\n{links_gdf[['A','B','geometry']]}")
    return links_gdf


def _set_links_df_index(links_df: pd.DataFrame) -> pd.DataFrame:
    """Sets the index of the links dataframe to be a copy of the primary key.

    Args:
        links_df (pd.DataFrame): links dataframe
    """
    if links_df.index.name != links_df.params.idx_col:
        links_df[links_df.params.idx_col] = links_df[links_df.params.primary_key]
        links_df = links_df.set_index(links_df.params.idx_col)
    return links_df


@check_output(LinksSchema, inplace=True)
def _links_data_to_links_df(
    links_df: Union[pd.DataFrame, List[dict]],
    links_crs: int = 4326,
    links_params: LinksParams = None,
    nodes_df: gpd.GeoDataFrame = None,
    nodes_crs: int = 4326,
) -> gpd.GeoDataFrame:
    """Create a links dataframe from list of link properties + link geometries or associated nodes.

    Sets index to be a copy of the primary key.
    Validates output dataframe using LinksSchema.

    Args:
        links_df (pd.DataFrame): df or list of dictionaries of link properties
        links_crs: coordinate reference system id for incoming links if geometry already exists.
            Defaults to 4326. Will convert everything to 4326 if it doesn't match.
        links_params: a LinkParams instance. Defaults to a default LinkParams instance..
        nodes_df: Associated notes geodataframe to use if geometries or location references not
            present. Defaults to None.
        nodes_crs: coordinate reference system id for incoming nodes if geometry already exists.
            Defaults to 4326. Will convert everything to 4326 if it doesn't match.
    Returns:
        pd.DataFrame: _description_
    """
    # Make it a dataframe it if isn't already
    if not isinstance(links_df, pd.DataFrame):
        links_df = pd.DataFrame(links_df)

    # If already has geometry, try coercing to a geodataframe.
    if not isinstance(links_df, gpd.GeoDataFrame) and "geometry" in links_df:
        gpd.GeoDataFrame(links_df, geometry="geometry", crs=links_crs)

    # check CRS and convert if necessary to 4326
    if isinstance(links_df, gpd.GeoDataFrame) and links_df.crs != 4326:
        links_df = links_df.to_crs(4326)
    if nodes_df is not None and nodes_df.crs != 4326:
        nodes_df = nodes_df.to_crs(4326)

    # If missing parameters, fill them in
    if "params" not in links_df.__dict__ or links_df.params is None:
        if links_params is None:
            links_df.__dict__["params"] = LinksParams()
        else:
            links_df.__dict__["params"] = links_params
        # need to add params to _metadata in order to make sure it is copied.
        # see: https://stackoverflow.com/questions/50372509/
        links_df._metadata += ["params"]
    WranglerLogger.debug(f"Link Params: {links_df.params}")
    # Set link  index
    links_df = _set_links_df_index(links_df)

    # If missing geometry, fill it and make it a GeoDataFrame
    if (
        not isinstance(links_df, gpd.GeoDataFrame)
        or links_df.geometry.isnull().values.any()
    ):
        if nodes_df is None:
            raise LinkCreationError(
                "Must give nodes_df argument if don't have Geometry"
            )
        links_df = _add_link_geometries_from_nodes(links_df, nodes_df)

    # If missing distance, approximate it from geometry
    if "distance" not in links_df:
        links_df["distance"] = length_of_linestring_miles(links_df)
    elif links_df["distance"].isnull().values.any():
        _add_dist = links_df["distance"].isnull()
        links_df[_add_dist] = length_of_linestring_miles(links_df.loc[_add_dist])

    links_df.gdf_name = "network_links"
    assert "params" in links_df.__dict__
    _disp_c = [
        links_df.params.primary_key,
        links_df.params.from_node,
        links_df.params.to_node,
        "name",
        "geometry",
    ]

    _num_links = len(links_df)
    if _num_links < 10:
        WranglerLogger.debug(f"New Links:\n{links_df[_disp_c]}")
    else:
        WranglerLogger.debug(f"{len(links_df)} new links.")

    return links_df


def shape_id_from_link_geometry(
    links_df: pd.DataFrame,
) -> gpd.GeoDataFrame:
    shape_ids = links_df["geometry"].apply(create_unique_shape_id)
    return shape_ids


@pd.api.extensions.register_dataframe_accessor("true_shape")
class TrueShapeAccessor:
    def __init__(self, links_df: LinksSchema):
        self._links_df = links_df

    def __call__(self, shapes_df: ShapesSchema):
        WranglerLogger.debug("Creating true shape from links and shapes.")
        WranglerLogger.debug(f"shapes_df:\n{shapes_df}")
        links_df = self._links_df.merge(
            shapes_df[[shapes_df.params.primary_key, "geometry"]],
            left_on=self._links_df.params.fk_to_shape,
            right_on=shapes_df.params.primary_key,
            how="left",
        )
        return links_df


@pd.api.extensions.register_dataframe_accessor("mode_query")
class ModeLinkAccessor:
    def __init__(self, links_df):
        self._links_df = links_df
        if links_df.params.table_type != "links":
            raise NotLinksError("`mode_query` is only available to links dataframes.")

    def __call__(self, modes: List[str]):
        # filter the rows where drive_access is True
        if "any" in modes:
            return self._links_df
        if isinstance(modes, str):
            modes = [modes]
        _mode_link_props = list(
            set(
                [
                    m
                    for m in modes
                    for m in self._links_df.params.modes_to_network_link_variables[m]
                ]
            )
        )

        modal_links_df = self._links_df.loc[
            self._links_df[_mode_link_props].any(axis=1)
        ]
        return modal_links_df


@pd.api.extensions.register_dataframe_accessor("of_type")
class LinkOfTypeAccessor:
    def __init__(self, links_df):
        self._links_df = links_df
        if links_df.params.table_type != "links":
            raise NotLinksError("`of_type` is only available to links dataframes.")

    @property
    def managed(self):
        return self._links_df.loc[self._links_df["managed"] == 1]

    @property
    def parallel_general_purpose(self):
        return self._links_df.loc[self._links_df["managed"] == -1]

    @property
    def general_purpose(self):
        return self._links_df.loc[self._links_df["managed"] < 1]

    @property
    def access_dummy(self):
        return self._links_df.loc[self._links_df["roadway"] == "ml_access"]

    @property
    def egress_dummy(self):
        return self._links_df.loc[self._links_df["roadway"] == "ml_egress"]

    @property
    def dummy(self):
        return self._links_df.loc[
            (self._links_df["roadway"] == "ml_access")
            | (self._links_df["roadway"] == "ml_egress")
        ]

    @property
    def pedbike_only(self):
        return self._links_df.loc[
            (
                (self._links_df["walk_access"].astype(bool))
                | (
                    (self._links_df["bike_access"].astype(bool))
                    & ~(self._links_df["drive_access"].astype(bool))
                )
            )
        ]

    @property
    def transit_only(self):
        return self._links_df.loc[
            (self._links_df["bus_only"].astype(bool))
            | (self._links_df["rail_only"].astype(bool))
        ]

    @property
    def drive_access(self):
        return self._links_df.loc[self._links_df["drive_access"].astype(bool)]

    @property
    def summary(self):
        d = {
            "total links": len(self._links_df),
            "managed": len(self.managed),
            "general_purpose": len(self.general_purpose),
            "access": len(self.access_dummy),
            "egress": len(self.egress_dummy),
            "pedbike only": len(self.pedbike_only),
            "transit only": len(self.transit_only),
            "drive access": len(self.drive_access),
        }
        return d


@check_input(LinksSchema, obj_getter="links_df", inplace=True)
def write_links(
    links_df: gpd.GeoDataFrame,
    out_dir: Union[str, Path] = ".",
    prefix: str = "",
    file_format: str = "json",
    overwrite: bool = False,
    include_geometry: bool = False,
) -> None:
    if not include_geometry and file_format == "geojson":
        file_format = "json"

    links_file = Path(out_dir) / f"{prefix}link.{file_format}"

    if not include_geometry:
        links_df = pd.DataFrame(links_df)
        links_df = links_df.drop(columns=["geometry"])

    write_table(links_df, links_file, overwrite=overwrite)


@pd.api.extensions.register_dataframe_accessor("set_link_prop")
class SetLinkPropAccessor:
    def __init__(self, links_df):
        self._links_df = links_df
        if links_df.params.table_type != "links":
            raise NotLinksError("`set_link_prop` is only available to links dataframs.")

    @staticmethod
    def _updated_default(existing_val, prop_dict):
        if "set" in prop_dict:
            return {"default": prop_dict["set"]}
        elif "change" in prop_dict:
            if isinstance(existing_val, dict):
                if "default" not in existing_val:
                    WranglerLogger.error(
                        "cannot change existing default value when it is not set."
                    )
                    raise LinkChangeError
                _exist_default = float(existing_val["default"])
            else:
                _exist_default = float(existing_val)
            return {"default": _exist_default + float(prop_dict["change"])}
        elif isinstance(existing_val, dict) and "default" in existing_val:
            return {"default": existing_val["default"]}
        return {}

    def _set_val_for_group(self, existing_val, prop_dict: dict):
        """sets value of link property when group key is present.

        Args:
            existing_val: existing value
            prop_dict (dict): dictionary of following format:

            ```yaml
            - ML_price:
                set: 0
                group:
                    - category: ['sov']
                    timeofday:
                        - timespan:  ['6:00', '9:00']
                        set: 1.5
                        - timespan: ['16:00', '19:00']
                        set: 2.5
                    - category: ['hov2']
                    timeofday:
                        - timespan: ['6:00', '9:00']
                        set: 1.0
                        - timespan: ['16:00', '19:00']
                        set: 2.0
            ```

        Returns: dictionary in the following format:

            ```yaml
            default: 3
            timeofday: [
                {category: ...., timespan: ...,value...},
                {category: ...., timespan: ...,value...},
            ]
            ```

        """

        prop_value = {}
        prop_value.update(self._updated_default(existing_val, prop_dict))

        prop_value["timeofday"] = []

        for g in prop_dict["group"]:
            category = g["category"]
            for tod in g["timeofday"]:
                if "set" in tod:
                    prop_value["timeofday"].append(
                        {
                            "category": category,
                            "timespan": parse_timespans_to_secs(tod["timespan"]),
                            "value": tod["set"],
                        }
                    )
                elif "change" in tod:
                    if not type(existing_val) in [int, float]:
                        raise LinkChangeError(
                            "Change keyword invoked on non-numeric existing val"
                        )
                    prop_value["timeofday"].append(
                        {
                            "category": category,
                            "timespan": parse_timespans_to_secs(tod["timespan"]),
                            "value": existing_val + tod["change"],
                        }
                    )
                else:
                    raise LinkChangeError("Change or Set not found in link change")
        return prop_value

    def _set_val_for_timeofday(self, existing_val, prop_dict: dict):
        """sets value of link property when timeofday key is present.

        Args:
            existing_val: existing value
            prop_dict (dict): dictionary of following format:

            ```yaml
            lanes:
                set: 3
                timeofday:
                    - timespan: ['6:00', '9:00']
                    set: 2
                    - timespan: ['16:00', '19:00']
            ```

        Returns: dictionary in following format:

            ```yaml
            default: 3
            timeofday: [
                {timespan: ...,value...},
                {timespan: ...,value...},
            ]
            ```

        """
        prop_value = {}
        prop_value.update(self._updated_default(existing_val, prop_dict))

        prop_value["timeofday"] = []
        WranglerLogger.debug(f"prop_dict['timeofday']: {prop_dict['timeofday']}")
        for tod in prop_dict["timeofday"]:
            if "set" in tod:
                prop_value["timeofday"].append(
                    {
                        "timespan": parse_timespans_to_secs(tod["timespan"]),
                        "value": tod["set"],
                    }
                )
            elif "change" in tod:
                if not type(existing_val) in [int, float]:
                    raise LinkChangeError(
                        "Change keyword invoked on non-numeric existing val"
                    )
                prop_value["timeofday"].append(
                    {
                        "timespan": parse_timespans_to_secs(tod["timespan"]),
                        "value": existing_val + tod["change"],
                    }
                )
            else:
                raise LinkChangeError("Change or Set not found in link change")
        return prop_value

    def __call__(
        self,
        link_idx: list,
        prop_name: str,
        prop_dict: dict,
        existing_value_conflict_error: bool = False,
    ) -> pd.DataFrame:
        """Sets the value of a link property.

        args:
            link_idx: list of link indices to change
            prop_name: property name to change
            prop_dict: dictionary of value from project_card
            existing_value_conflict_error: If True, will trigger an error if the existing
                specified value in the project card doesn't match the value in links_df.
                Otherwise, will only trigger a warning. Defaults to False.

        """

        # check existing if necessary
        if "existing" in prop_dict:
            if prop_name not in self._links_df.columns:
                WranglerLogger.warning(f"No existing value defined for {prop_name}")
                if existing_value_conflict_error:
                    raise LinkChangeError(f"No existing value defined for {prop_name}")
            elif (
                not self._links_df.loc[link_idx, prop_name]
                .eq(prop_dict["existing"])
                .all()
            ):
                WranglerLogger.warning(
                    f"Existing value defined for {prop_name} in project card \
                    does not match the value in the roadway network for the selected links. \n\
                    Specified Existing:{prop_dict['existing']}\n\
                    Actual Existing:\n {self._links_df.loc[link_idx,prop_name]}."
                )

                if existing_value_conflict_error:
                    raise LinkChangeError(
                        "Conflict between specified existing and actual existing values"
                    )

        _links_df = self._links_df.copy()
        # if it is a new attribute then initialize with NaN values
        if prop_name not in self._links_df:
            _links_df[prop_name] = np.NaN
        if len(link_idx) < 6:
            WranglerLogger.debug(
                f"Existing {prop_name}:\n{_links_df.loc[link_idx, prop_name]}"
            )
        # if a default specified, set NaN to the deafult
        if "default" in prop_dict:
            _default_val = coerce_val_to_series_type(
                prop_dict["default"], _links_df[prop_name]
            )
            _links_df[prop_name] = _links_df[prop_name].fillna(_default_val)

        if "group" in prop_dict:
            _links_df.loc[link_idx, prop_name] = _links_df.loc[
                link_idx, prop_name
            ].apply(self._set_val_for_group, prop_dict=prop_dict)

        elif "timeofday" in prop_dict:
            _links_df.loc[link_idx, prop_name] = _links_df.loc[
                link_idx, prop_name
            ].apply(self._set_val_for_timeofday, prop_dict=prop_dict)

        elif "set" in prop_dict:
            _links_df.loc[link_idx, prop_name] = coerce_val_to_series_type(
                prop_dict["set"], _links_df[prop_name]
            )

        elif "change" in prop_dict:
            _links_df.loc[link_idx, prop_name] = _links_df.loc[
                link_idx, prop_name
            ].apply(lambda x: x + float(prop_dict["change"]))

        else:
            raise LinkChangeError(
                "Couldn't find correct link change spec in: {prop_dict}"
            )

        return _links_df


class NotLinksError(Exception):
    pass


class LinkChangeError(Exception):
    pass


class LinkCreationError(Exception):
    pass
