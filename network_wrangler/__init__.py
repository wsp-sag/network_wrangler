__version__ = "0.0.0"

import os
from datetime import datetime

from .logger import WranglerLogger, setupLogging
from .projectcard import ProjectCard
from .roadwaynetwork import RoadwayNetwork
from .transitnetwork import TransitNetwork
from .scenario import Scenario
from .scenario import net_to_mapbox

__all__ = [
    "WranglerLogger",
    "setupLogging",
    "ProjectCard",
    "RoadwayNetwork",
    "TransitNetwork",
    "Scenario",
]

setupLogging(
    log_filename=os.path.join(
        os.getcwd(),
        "network_wrangler_{}.log".format(datetime.now().strftime("%Y_%m_%d__%H_%M_%S")),
    ),
    log_to_console=True,
)
