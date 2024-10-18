"""Network Wrangler Package."""

__version__ = "1.0-beta.2"

import warnings

# Suppress the specific FutureWarning from geopandas
warnings.filterwarnings(
    "ignore", category=FutureWarning, message=".*convert_dtype parameter is deprecated.*"
)

from .configs import load_wrangler_config
from .logger import WranglerLogger, setup_logging
from .roadway.io import load_roadway, load_roadway_from_dir, write_roadway
from .scenario import Scenario, create_scenario, load_scenario
from .transit.io import load_transit, write_transit
from .utils.df_accessors import *

__all__ = [
    "WranglerLogger",
    "setup_logging",
    "load_transit",
    "write_transit",
    "load_roadway",
    "load_roadway_from_dir",
    "write_roadway",
    "create_scenario",
    "Scenario",
    "load_wrangler_config",
    "load_scenario",
]


TARGET_ROADWAY_NETWORK_SCHEMA_VERSION = "1"
TARGET_TRANSIT_NETWORK_SCHEMA_VERSION = "1"
TARGET_PROJECT_CARD_SCHEMA_VERSION = "1"

MIN_ROADWAY_NETWORK_SCHEMA_VERSION = "0"
MIN_TRANSIT_NETWORK_SCHEMA_VERSION = "0"
MIN_PROJECT_CARD_SCHEMA_VERSION = "1"
