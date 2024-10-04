"""Network Wrangler Package."""

__version__ = "0.0.0"

from .logger import WranglerLogger, setup_logging
from .roadway.io import load_roadway, load_roadway_from_dir, write_roadway
from .transit.io import load_transit, write_transit
from .scenario import Scenario, create_scenario, load_scenario
from .utils.df_accessors import *
from .configs import load_wrangler_config

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
    "load_scenario"
]


TARGET_ROADWAY_NETWORK_SCHEMA_VERSION = "1"
TARGET_TRANSIT_NETWORK_SCHEMA_VERSION = "1"
TARGET_PROJECT_CARD_SCHEMA_VERSION = "1"

MIN_ROADWAY_NETWORK_SCHEMA_VERSION = "0"
MIN_TRANSIT_NETWORK_SCHEMA_VERSION = "0"
MIN_PROJECT_CARD_SCHEMA_VERSION = "1"
