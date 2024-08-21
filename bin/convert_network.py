#!/usr/bin/env python3
"""Convert a network from one serialization format to another.

Usage: python convert_network.py <input_path> <network_type> <output_format> <out_dir>\
    [--input_suffix <input_suffix>] [--out_prefix <prefix>] [-o].

Arguments:
    input_path        Path to the input network directory.

    network_type      Determine if transit or roadway network.
    output_format     Format to write to. Options: parquet, geojson, csv.
    out_dir           Path to the output directory where the trimmed network will be saved.

Options:
    --input_suffix          Filetype to read in. Defaults to geojson for roadway and csv for
        transit.
    --out_prefix <prefix>   Prefix for the output file name. Defaults to ''.
    -o                      Overwrite the output file if it exists. Default to not overwrite.
"""

import argparse
from pathlib import Path

import sys

from network_wrangler.roadway.io import convert_roadway_file_serialization
from network_wrangler.transit.io import convert_transit_serialization
from network_wrangler import WranglerLogger


def convert(
    input_path,
    network_type,
    output_format,
    out_dir,
    input_suffix,
    out_prefix,
    overwrite,
):
    """Wrapper function to convert network serialization formats."""
    if network_type == "transit":
        convert_transit_serialization(
            input_path,
            output_format,
            out_dir,
            input_suffix,
            out_prefix,
            overwrite,
        )
    elif network_type == "roadway":
        convert_roadway_file_serialization(
            input_path,
            output_format,
            out_dir,
            input_suffix,
            out_prefix,
            overwrite,
        )
    else:
        raise ValueError("Network type unrecognized. Please use 'transit' or 'roadway'")


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
        "output_format",
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
        "--input_suffix",
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
            args.output_format,
            args.out_dir,
            args.input_suffix,
            args.out_prefix,
            args.o,
        )
    except Exception as e:
        WranglerLogger.error(f"Convert_networks error: {e}")
        sys.exit(1)
