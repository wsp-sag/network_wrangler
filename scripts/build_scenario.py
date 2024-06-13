"""Build a scenario from a base network and a project card file.

This script takes a base network and a project card file as inputs and creates a scenario.
The scenario includes the base network and applies projects specified in the project card
file to modify the network.

Usage:
    python build_scenario.py <config_file>

Arguments:
    config_file (str): Path to the configuration file specifying the base network, project card
        file, and other scenario parameters.

Example:
    python build_scenario.py config.yaml

The configuration file should be in YAML format and include the following information:
- base_network: Information about the base network, including input directory, shape file name,
    link file name, node file name, and whether to validate the network.
- scenario: Information about the scenario, including the project card file path, tags to filter
    projects, whether to write out the modified network, output directory, and output prefix.

The script reads the configuration file, creates a base scenario using the base network
information, and then creates a scenario by applying projects from the project card file to
the base scenario. The modified network can be written out to files if specified.
"""

import sys
import yaml
import warnings

from pathlib import Path

from network_wrangler import Scenario, write_roadway
from network_wrangler.scenario import create_base_scenario
from network_wrangler.logger import WranglerLogger


warnings.filterwarnings("ignore")

if __name__ == "__main__":
    args = sys.argv

    if len(args) == 1:
        raise ValueError("ERROR - config file must be passed as an argument!!!")

    config_file = args[1]

    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Specified config file does not exist - {config_path}")

    with open(config_file, "r") as config:
        config_dict = yaml.safe_load(config)

    base_network_dir = config_dict.get("base_network").get("input_dir")
    base_shape_name = config_dict.get("base_network").get("shape_file_name")
    base_link_name = config_dict.get("base_network").get("link_file_name")
    base_node_name = config_dict.get("base_network").get("node_file_name")
    validate_base_network = config_dict.get("base_network").get("validate_network")

    project_card_filepath = config_dict.get("scenario").get("project_card_filepath")
    project_tags = config_dict.get("scenario").get("tags")

    write_out = config_dict.get("scenario").get("write_out")
    out_dir = config_dict.get("scenario").get("output_dir")
    out_prefix = config_dict.get("scenario").get("out_prefix")

    if project_tags is None:
        project_tags = []

    if project_card_filepath is None:
        project_card_filepath = []

    # Create Base Network
    base_scenario = create_base_scenario(
        roadway_dir=base_network_dir,
        base_shape_name=base_shape_name,
        base_link_name=base_link_name,
        base_node_name=base_node_name,
        transit_dir=base_network_dir,
    )

    my_scenario = Scenario.create_scenario(
        base_scenario=base_scenario,
        project_card_filepath=project_card_filepath,
        filter_tags=project_tags,
    )

    WranglerLogger("Applying these projects to the base scenario ...")
    WranglerLogger("\n".join(my_scenario.projects.keys()))

    my_scenario.apply_all_projects()

    if write_out:
        write_roadway(my_scenario.road_net, prefix=out_prefix, out_dir=out_dir)
        my_scenario.transit_net.write(filename=out_prefix, path=out_dir)
