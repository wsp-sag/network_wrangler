#!/usr/bin/env python
"""Convert a network from one serialization format to another.

Usage: python convert_network.py <input_path> <network_type> <output_format> <out_dir>\
    [--input_file_format <input_file_format>] [--out_prefix <prefix>] [-o].

Arguments:
    input_path        Path to the input network directory.

    network_type      Determine if transit or roadway network.
    output_format     Format to write to. Options: parquet, geojson, csv.
    out_dir           Path to the output directory where the trimmed network will be saved.

Options:
    --input_file_format          Filetype to read in. Defaults to geojson for roadway and csv for
        transit.
    --out_prefix <prefix>   Prefix for the output file name. Defaults to ''.
    -o                      Overwrite the output file if it exists. Default to not overwrite.
"""

import argparse
import sys
from pathlib import Path

from network_wrangler import WranglerLogger
from network_wrangler.roadway.io import convert_roadway_file_serialization
from network_wrangler.transit.io import convert_transit_serialization


def convert(
    input_path,
    network_type,
    out_file_format,
    out_dir,
    input_file_format,
    out_prefix,
    overwrite,
):
    """Wrapper function to convert network serialization formats."""
    if network_type == "transit":
        convert_transit_serialization(
            input_path,
            out_file_format,
            out_dir,
            input_file_format,
            out_prefix,
            overwrite,
        )
    elif network_type == "roadway":
        convert_roadway_file_serialization(
            input_path,
            out_file_format,
            out_dir,
            input_file_format,
            out_prefix,
            overwrite,
        )
    else:
        msg = "Network type unrecognized. Please use 'transit' or 'roadway'"
        raise ValueError(msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert a network from one serialization format to another."
    )
    parser.add_argument("input_path", type=Path, help="Path to the input network directory.")

    parser.add_argument(
        "network_type",
        type=str,
        choices=["transit", "roadway"],
        help="Determine if transit or roadway network.",
    )
    parser.add_argument(
        "out_file_format",
        type=str,
        choices=["parquet", "geojson", "csv"],
        help="Format to write to.",
    )
    parser.add_argument(
        "out_dir",
        type=Path,
        help="Path to the output directory where the trimmed network will be saved.",
    )
    parser.add_argument(
        "--input_file_format",
        type=str,
        choices=["parquet", "geojson", "csv"],
        default=None,
        help="Filetype to read in. Defaults to geojson for roadway and csv for transit.",
    )
    parser.add_argument(
        "--out_prefix", type=str, default="", help="Prefix for the output file name."
    )
    parser.add_argument("-o", action="store_true", help="Overwrite the output file if it exists")
    args = parser.parse_args()
    try:
        convert(
            args.input_path,
            args.network_type,
            args.out_file_format,
            args.out_dir,
            args.input_file_format,
            args.out_prefix,
            args.o,
        )
    except Exception as e:
        WranglerLogger.error(f"Convert_networks error: {e}")
        sys.exit(1)
