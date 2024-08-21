"""Dataframe accessor shortcuts for RoadLinksTables allowing for easy filtering and editing."""

from typing import List

import pandas as pd

from ...logger import WranglerLogger
from ...models.roadway.tables import RoadLinksTable, RoadShapesTable
from .filters import (
    filter_links_managed_lanes,
    filter_links_parallel_general_purpose,
    filter_links_general_purpose,
    filter_links_access_dummy,
    filter_links_egress_dummy,
    filter_links_dummy,
    filter_links_pedbike_only,
    filter_links_transit_only,
    filter_links_transit_access,
    filter_links_drive_access,
    filter_links_to_modes,
    filter_link_properties_managed_lanes,
    filter_links_general_purpose_no_parallel_managed,
)
from .links import NotLinksError
from .summary import link_summary
from .geo import true_shape


@pd.api.extensions.register_dataframe_accessor("of_type")
class LinkOfTypeAccessor:
    """Wrapper for various filters of RoadLinksTable.

    Methods:
        links_df.of_type.managed: filters links dataframe to only include managed lanes.
        links_df.of_type.parallel_general_purpose: filters links dataframe to only include
            general purpose links parallel to managed.
        links_df.of_type.general_purpose: filters links dataframe to only include all general
            purpose links.
        links_df.of_type.general_purpose_no_parallel_managed: filters links dataframe to only
            include general purpose links without parallel managed lanes.
        links_df.of_type.access_dummy: filters links dataframe to only include all access dummy
            links connecting managed lanes.
        links_df.of_type.egress_dummy: filters links dataframe to only include all egress dummy
            links connecting managed lanes.
        links_df.of_type.dummy: filters links dataframe to only include all dummy links
            connecting managed lanes.
        links_df.of_type.pedbike_only: filters links dataframe to only include all links that
            only ped/bikes can be on.
        links_df.of_type.transit_only: filters links dataframe to only include all links that
            only transit can be on.
        links_df.of_type.transit_access: filters links dataframe to only include all links
            that transit can access.
        links_df.of_type.drive_access: filters links dataframe to only include all links
            that drive can access.
        links_df.of_type.summary_df: returns a summary of the links dataframe.

    """

    def __init__(self, links_df):
        """LinkOfTypeAccessor for RoadLinksTable."""
        self._links_df = links_df
        try:
            links_df.params.table_type == "links"
        except AttributeError:
            WranglerLogger.warning(
                "`of_type` should only be used on links dataframes. \
                No params.table_type found."
            )
        except AssertionError:
            WranglerLogger.warning(
                f"`of_type` should only be used on links dataframes. \
                Found type: {links_df.params.table_type}"
            )
            raise NotLinksError("`of_type` is only available to links dataframes.")

    @property
    def managed(self):
        """Filters links dataframe to only include managed lanes."""
        return filter_links_managed_lanes(self._links_df)

    @property
    def parallel_general_purpose(self):
        """Filters links dataframe to general purpose links parallel to managed lanes."""
        ml_properties = filter_link_properties_managed_lanes(self._links_df)
        keep_c = [c for c in self._links_df.columns if c not in ml_properties]
        return filter_links_parallel_general_purpose(self._links_df[keep_c])

    @property
    def general_purpose(self):
        """Filters links dataframe to only include general purpose links."""
        ml_properties = filter_link_properties_managed_lanes(self._links_df)
        keep_c = [c for c in self._links_df.columns if c not in ml_properties]
        return filter_links_general_purpose(self._links_df[keep_c])

    @property
    def general_purpose_no_parallel_managed(self):
        """Filters links general purpose links without parallel managed lanes."""
        ml_properties = filter_link_properties_managed_lanes(self._links_df)
        keep_c = [c for c in self._links_df.columns if c not in ml_properties]
        return filter_links_general_purpose_no_parallel_managed(self._links_df[keep_c])

    @property
    def access_dummy(self):
        """Filters links dataframe to access dummy links connecting managed lanes."""
        return filter_links_access_dummy(self._links_df)

    @property
    def egress_dummy(self):
        """Filters links dataframe to egress dummy links connecting managed lanes."""
        return filter_links_egress_dummy(self._links_df)

    @property
    def dummy(self):
        """Filters links dataframe to dummy links connecting managed lanes."""
        return filter_links_dummy(self._links_df)

    @property
    def pedbike_only(self):
        """Filters links dataframe to links that only ped/bikes can be on."""
        return filter_links_pedbike_only(self._links_df)

    @property
    def transit_only(self):
        """Filters links dataframe to links that only transit can be on."""
        return filter_links_transit_only(self._links_df)

    @property
    def transit_access(self):
        """Filters links dataframe to all links that transit can access."""
        return filter_links_transit_access(self._links_df)

    @property
    def drive_access(self):
        """Filters links dataframe to only include all links that drive can access."""
        return filter_links_drive_access(self._links_df)

    @property
    def summary_df(self) -> pd.DataFrame:
        """Returns a summary of the links dataframe."""
        return link_summary(self._links_df)


@pd.api.extensions.register_dataframe_accessor("mode_query")
class ModeLinkAccessor:
    """Wrapper for filtering RoadLinksTable by modal ability: : links_df.mode_query(modes_list).

    Args:
        modes (List[str]): list of modes to filter by.
    """

    def __init__(self, links_df):
        """ModeLinkAccessor for RoadLinksTable."""
        self._links_df = links_df
        if links_df.params.table_type != "links":
            raise NotLinksError("`mode_query` is only available to links dataframes.")

    def __call__(self, modes: List[str]):
        """Filters links dataframe to  links that are accessible by the modes in the list."""
        return filter_links_to_modes(self._links_df, modes)


@pd.api.extensions.register_dataframe_accessor("true_shape")
class TrueShapeAccessor:
    """Wrapper for returning a gdf with true_shapes: links_df.true_shape(shapes_df)."""

    def __init__(self, links_df: RoadLinksTable):
        """TrueShapeAccessor for RoadLinksTable."""
        self._links_df = links_df
        if links_df.params.table_type != "links":
            raise NotLinksError("`true_shape` is only available to links dataframes.")

    def __call__(self, shapes_df: RoadShapesTable):
        """Updates geometry to have shape of shapes_df where available."""
        return true_shape(self._links_df, shapes_df)
