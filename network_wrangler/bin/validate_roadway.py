#!/usr/bin/env python
"""Validates a roadway network to the wrangler data model specifications from command line.

Usage:
    python validate_roadway.py <network_directory> <network_file_format> [-s] [--output_dir <output_dir>]

    network_directory: The roadway network file directory.
    network_file_format: The suffices of roadway network file name.
    -s, --strict: Validate the roadway network strictly without parsing and filling in data.
    --output_dir: The output directory for the validation report.
"""

import argparse
from datetime import datetime
from pathlib import Path

from network_wrangler import WranglerLogger, setup_logging
from network_wrangler.roadway.validate import validate_roadway_in_dir

if __name__ == "__main__":
    # ----- Setup Arguments ------
    parser = argparse.ArgumentParser(
        "Validate a roadway network to the wrangler data model specifications."
    )
    parser.add_argument(
        "network_directory", help="The roadway network file directory.", default="."
    )
    parser.add_argument(
        "network_file_format",
        help="The file format of roadway network file name.",
        default="geojson",
    )
    parser.add_argument(
        "-s",
        "--strict",
        action="store_true",
        help="Validate the roadway network strictly without parsing and filling in data.",
    )
    parser.add_argument(
        "--output_dir", help="The output directory for the validation report.", default="."
    )

    args = parser.parse_args()

    # ----- Setup Report ------
    dt_str = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
    report_path = Path(args.output_dir) / f"{dt_str}_roadway_validation_report.txt"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    WranglerLogger.info(f"Writing report to {report_path}")

    setup_logging(debug_log_filename=report_path, std_out_level="info")

    WranglerLogger.info(f"Validation Report for Roadway Network in {args.network_directory}\n")
    WranglerLogger.info(f"Validation Date: {datetime.now()}\n\n")
    WranglerLogger.info("Mode: Strict\n" if args.strict else "Mode: Non-Strict\n")

    # ----- Perform validation ------

    validate_roadway_in_dir(
        directory=args.network_directory,
        file_format=args.network_file_format,
        strict=argparse.strict,
        output_dir=args.output_dir,
    )
