#!/usr/bin/env python
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

import argparse
from pathlib import Path

from network_wrangler.scenario import build_scenario_from_config

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build a scenario from a configuration file.")
    parser.add_argument(
        "config_file",
        type=Path,
        help="Path to configuration file.",
    )
    args = parser.parse_args()

    build_scenario_from_config(args.config_file)
