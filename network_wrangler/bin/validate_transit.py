#!/usr/bin/env python
"""Validates a transit network to the wrangler data model specifications from command line.

Usage:
    python validate_transit.py <network_directory> <network_suffix> [-s] [--output_dir <output_dir>] [--road_dir <road_dir>] [--road_suffix <road_suffix>]

    network_directory: The transit network file directory.
    network_suffix: The suffices of transit network file name.
    -s, --strict: Validate the transit network strictly without parsing and filling in data.
    --output_dir: The output directory for the validation report.
    --road_dir: The directory roadway network if want to validate the transit network to it.
    --road_suffix: The suffix for roadway network. Defaults to 'geojson'.
"""

import argparse
from datetime import datetime

from pathlib import Path

from network_wrangler import WranglerLogger, setup_logging
from network_wrangler.transit.validate import validate_transit_in_dir


if __name__ == "__main__":
    # ----- Setup Arguments ------
    parser = argparse.ArgumentParser(
        "Validate a transit network to the wrangler data \
                                     model specifications."
    )
    parser.add_argument(
        "network_directory", help="The transit network file directory.", default="."
    )
    parser.add_argument(
        "network_suffix", help="The suffices of transit network file name.", default="geojson"
    )
    parser.add_argument(
        "-s",
        "--strict",
        action="store_true",
        help="Validate the transit network strictly without parsing and filling in data.",
    )
    parser.add_argument(
        "--road_dir",
        help="The directory roadway network if want to validate the transit network to it.",
        default=None,
    )
    parser.add_argument(
        "--road_suffix",
        help="The suffix for roadway network. Defaults to 'geojson'.",
        default="geojson",
    )
    parser.add_argument(
        "--output_dir", help="The output directory for the validation report.", default="."
    )
    parser.add_argument(
        "--output_dir", help="The output directory for the validation report.", default="."
    )

    args = parser.parse_args()

    # ----- Setup Report ------
    dt_str = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
    report_path = Path(args.output_dir) / f"{dt_str}_transit_validation_report.txt"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Writing report to {report_path}")

    setup_logging(debug_log_filename=report_path, std_out_level="info")

    WranglerLogger.info(f"Validation Report for Transit Network in {args.network_directory}\n")
    WranglerLogger.info(f"Validation Date: {datetime.now()}\n\n")
    WranglerLogger.info("Mode: Strict\n" if args.strict else "Mode: Non-Strict\n")

    # ----- Perform validation ------

    validate_transit_in_dir(
        directory=args.network_directory,
        suffix=args.network_suffix,
        road_dir=args.road_dir,
        road_suffix=args.road_suffix,
    )
