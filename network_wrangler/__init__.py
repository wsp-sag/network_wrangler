__version__ = "0.0.0"

import os
import sys
from datetime import datetime

from .logger import WranglerLogger, setupLogging
from .projectcard import ProjectCard
from .roadwaynetwork import RoadwayNetwork
from .transitnetwork import TransitNetwork
from .scenario import Scenario
from .scenario import net_to_mapbox
from .utils import point_df_to_geojson, link_df_to_json, make_slug
from .utils import parse_time_spans, offset_location_reference, haversine_distance
from .utils import create_unique_shape_id
from .utils import create_location_reference_from_nodes
from .utils import create_line_string
from .utils import update_df

__all__ = [
    "WranglerLogger",
    "setupLogging",
    "ProjectCard",
    "RoadwayNetwork",
    "point_df_to_geojson",
    "TransitNetwork",
    "Scenario",
    "link_df_to_json",
    "make_slug",
    "parse_time_spans",
    "offset_location_reference",
    "haversine_distance",
    "create_unique_shape_id",
    "create_location_reference_from_nodes",
    "create_line_string",
    "net_to_mapbox",
    "update_df",
]

now_str = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
log_filename = f"network_wrangler_{now_str}.log"

log_dir = os.path.join(os.getcwd(), "logs")
if not os.path.exists(log_dir):
    os.mkdir("logs")

setupLogging(
    log_filename=os.path.join(
        log_dir,
        log_filename,
    ),
    log_to_console=True,
)
