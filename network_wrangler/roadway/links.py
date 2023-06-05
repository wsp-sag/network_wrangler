import json
import os

from dataclasses import dataclass, field
from typing import Union, Mapping, Any, Optional, List

import geopandas as gpd
import numpy as np
import pandas as pd
import pandera as pa

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError

import pandera as pa
from pandera import check_input, check_output, DataFrameModel
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries

from ..logger import WranglerLogger

from .utils import create_unique_shape_id
from ..utils import (
    line_string_from_location_references,
    coerce_val_to_series_type,
    parse_time_spans_to_secs,
    line_string_from_location_references,
    location_reference_from_nodes,
)

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


class LinksSchema(DataFrameModel):
    """Datamodel used to validate if links_df is of correct format and types."""

    model_link_id: Series[int] = pa.Field(coerce=True, unique=True)
    A: Series[Any] = pa.Field(nullable=False)
    B: Series[Any] = pa.Field(nullable=False)
    geometry: GeoSeries = pa.Field(nullable=False)
    name: Series[str] = pa.Field(nullable=False)
    rail_only: Series[bool] = pa.Field(coerce=True, nullable=False)
    bus_only: Series[bool] = pa.Field(coerce=True, nullable=False)
    drive_access: Series[bool] = pa.Field(coerce=True, nullable=False)
    bike_access: Series[bool] = pa.Field(coerce=True, nullable=False)
    walk_access: Series[bool] = pa.Field(coerce=True, nullable=False)

    roadway: Series[str] = pa.Field(nullable=False)
    lanes: Series[int] = pa.Field(coerce=True, nullable=False)

    # Optional Fields
    truck_access: Optional[Series[bool]] = pa.Field(coerce=True, nullable=True)
    osm_link_id: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
    locationReferences: Optional[Series[Any]] = pa.Field(nullable=True)
    shape_id: Optional[Series[Any]] = pa.Field(nullable=True)

    @pa.dataframe_check
    def unique_ab(cls, df: pd.DataFrame) -> bool:
        return ~df[["A", "B"]].duplicated()


@check_output(LinksSchema, inplace=True)
def read_links(
    filename: str, crs: int = 4326, links_params: Union[dict, LinksParams] = None
) -> gpd.GeoDataFrame:
    """Reads links and returns a geodataframe of links.

    Sets index to be a copy of the primary key.
    Validates output dataframe using LinksSchema.

    Args:
        filename (str): file to read links in from.
        crs: coordinate reference system number. Defaults to 4323.
        link_params: a LinkParams instance. Defaults to a default LinkParams instance.
    """
    WranglerLogger.info(f"Reading links from {filename}.")
    with open(filename) as f:
        links_data = json.load(f)

    links_df = links_data_to_links_df(links_data, crs=crs, links_params=links_params)
    links_df.params.source_file = filename
    # need to add params to _metadata in order to make sure it is copied.
    # see: https://stackoverflow.com/questions/50372509/why-are-attributes-lost-after-copying-a-pandas-dataframe/50373364#50373364

    return links_df


@check_output(LinksSchema, inplace=True)
def links_data_to_links_df(
    links_data: List[dict],
    crs: int = 4326,
    links_params: LinksParams = None,
    link_geometries: List = None,
    nodes_df: gpd.GeoDataFrame = None,
) -> gpd.GeoDataFrame:
    """Creates a links dataframe from list of link properties + link geometries or associated nodes.

    Sets index to be a copy of the primary key.
    Validates output dataframe using LinksSchema.

    Args:
        links_data (pd.DataFrame): _description_
        crs: coordinate reference system id. Defaults to 4326
        links_params: a LinkParams instance. Defaults to a default LinkParams instance.
        link_geometies: list geometry data. Defaults to None.
        nodes_df: Associated notes geodataframe to use if geometries or location references not
            present. Defaults to None.
    Returns:
        pd.DataFrame: _description_
    """
    if links_params is None:
        links_params = LinksParams()

    if link_geometries is None:
        temp_links_df = pd.DataFrame(links_data)

        if not "locationReferences" in temp_links_df:
            if nodes_df is None:
                raise LinkCreationError(
                    "Must give nodes_df argument if don't have LocationReferences or Geometry"
                )
            temp_links_df.__dict__["params"] = links_params
            temp_links_df["locationReferences"] = location_references_from_nodes(
                temp_links_df, nodes_df
            )

        link_geometries = temp_links_df["locationReferences"].apply(
            line_string_from_location_references
        )

    links_df = gpd.GeoDataFrame(links_data, geometry=link_geometries)
    links_df.crs = crs
    links_df.gdf_name = "network_links"
    links_df.__dict__["params"] = links_params

    links_df[links_df.params.idx_col] = links_df[links_df.params.primary_key]
    links_df.set_index(links_df.params.idx_col, inplace=True)

    links_df._metadata += ["params"]

    _disp_c = [
        links_df.params.primary_key,
        links_df.params.from_node,
        links_df.params.to_node,
        "name",
    ]
    WranglerLogger.debug(f"New Links:\n{links_df[_disp_c]}")

    return links_df


def shape_id_from_link_geometry(
    links_df: pd.DataFrame,
) -> gpd.GeoDataFrame:
    shape_ids = links_df["geometry"].apply(create_unique_shape_id)
    return shape_ids


def location_references_from_nodes(
    links_df: pd.DataFrame, nodes_df: pd.DataFrame
) -> pd.Series:
    locationreferences_s = links_df.apply(
        lambda x: location_reference_from_nodes(
            [
                nodes_df.loc[nodes_df.index == x[links_df.params.from_node]].squeeze(),
                nodes_df[nodes_df.index == x[links_df.params.to_node]].squeeze(),
            ]
        ),
        axis=1,
    )
    return locationreferences_s


def validate_wrangler_links_file(
    link_file, schema_location: str = "roadway_network_link.json"
):
    """
    Validate roadway network data link schema and output a boolean
    """

    if not os.path.exists(schema_location):
        base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "schemas")
        schema_location = os.path.join(base_path, schema_location)

    with open(schema_location) as schema_json_file:
        schema = json.load(schema_json_file)

    with open(link_file) as link_json_file:
        json_data = json.load(link_json_file)

    try:
        validate(json_data, schema)
        return True

    except ValidationError as exc:
        WranglerLogger.error("Failed Link schema validation: Validation Error")
        WranglerLogger.error("Link File Loc:{}".format(link_file))
        WranglerLogger.error("Path:{}".format(exc.path))
        WranglerLogger.error(exc.message)

    except SchemaError as exc:
        WranglerLogger.error("Invalid Link Schema")
        WranglerLogger.error("Link Schema Loc: {}".format(schema_location))
        WranglerLogger.error(json.dumps(exc.message, indent=2))

    return False


@pd.api.extensions.register_dataframe_accessor("mode_query")
class ModeLinkAccessor:
    def __init__(self, links_df):
        self._links_df = links_df

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
class ModeLinkAccessor:
    def __init__(self, links_df):
        self._links_df = links_df

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


@check_input(LinksSchema, inplace=True)
def links_df_to_json(links_df: pd.DataFrame, properties: list):
    """Export pandas dataframe as a json object.

    Modified from: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/

    Args:
        df: Dataframe to export
        properties: list of properties to export
    """

    # can't remember why we need this?
    if "distance" in properties:
        links_df["distance"].fillna(0)

    json = []
    for _, row in links_df.iterrows():
        feature = {}
        for prop in properties:
            feature[prop] = row[prop]
        json.append(feature)

    return json


@pd.api.extensions.register_dataframe_accessor("set_link_prop")
class ModeLinkAccessor:
    def __init__(self, links_df):
        self._links_df = links_df

    @staticmethod
    def _updated_default(existing_val, prop_dict):
        if "set" in prop_dict:
            return {"default": prop_dict["set"]}
        elif "change" in prop_dict:
            if isinstance(existing_val, dict):
                if not "default" in existing_val:
                    WranglerLogger.error(
                        "cannot change existing default value when it is not set."
                    )
                    raise LinkChangeError
                _exist_default = float(existing_val["default"])
            else:
                _exist_default = float(existing_val)
            return {"default": _exist_default + float(prop_dict["change"])}
        elif isinstance(existing_val, dict):
            if "default" in existing_val:
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
                        - time: ['6:00', '9:00']
                        set: 1.5
                        - time: ['16:00', '19:00']
                        set: 2.5
                    - category: ['hov2']
                    timeofday:
                        - time: ['6:00', '9:00']
                        set: 1.0
                        - time: ['16:00', '19:00']
                        set: 2.0
            ```

        Returns: dictionary in the following format:

            ```yaml
            default: 3
            timeofday: [
                {category: ...., time: ...,value...},
                {category: ...., time: ...,value...},
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
                            "time": parse_time_spans_to_secs(tod["time"]),
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
                            "time": parse_time_spans_to_secs(tod["time"]),
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
                    - time: ['6:00', '9:00']
                    set: 2
                    - time: ['16:00', '19:00']
            ```

        Returns: dictionary in following format:

            ```yaml
            default: 3
            timeofday: [
                {time: ...,value...},
                {time: ...,value...},
            ]
            ```

        """
        prop_value = {}
        prop_value.update(self._updated_default(existing_val, prop_dict))

        prop_value["timeofday"] = []

        for tod in prop_dict["timeofday"]:
            if "set" in tod:
                prop_value["timeofday"].append(
                    {
                        "time": parse_time_spans_to_secs(tod["time"]),
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
                        "time": parse_time_spans_to_secs(tod["time"]),
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
            if not prop_name in self._links_df.columns:
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


class LinkChangeError(Exception):
    pass


class LinkCreationError(Exception):
    pass
