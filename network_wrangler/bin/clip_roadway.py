#!/usr/bin/env python
"""Usage: python clip_roadway.py <network_path> <boundary> <out_dir> [--out_prefix <prefix>] [--out_format <format>].

This script trims a roadway network based on a given boundary and outputs the trimmed network.

Arguments:
- network_path: Path to the input roadway network directory.
- boundary: Path to the boundary file (shapefile or GeoJSON) or a geocode representing the
    boundary (e.g. "Raleigh, NC, USA").
- out_dir: Path to the output directory where the trimmed network will be saved.
- --out_prefix <prefix>: Prefix for the output file name. (optional)
- --out_format <format>: Output file format. Supported formats: 'geojson', 'parquet'.
    Default: 'geojson'. (optional)
- -o: Overwrite the output files if they already exist. Otherwise, will bork if files exist.
    (optional)
- -v: Increase verbocity of output. (optional)
- -q: Decrease verbocity of output. (optional)

Example usage:
python clip_roadway.py /path/to/network_dir /path/to/boundary.shp /path/to/output_dir
    --out_prefix clip --out_format geojson

"""

import argparse
import datetime
import logging
from pathlib import Path

from network_wrangler.logger import WranglerLogger
from network_wrangler.roadway.clip import clip_roadway
from network_wrangler.roadway.io import load_roadway_from_dir, write_roadway

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Trim a roadway network based on a given boundary."
    )
    parser.add_argument(
        "network_path",
        type=str,
        help="Path to the input roadway network directory.",
    )
    parser.add_argument(
        "boundary",
        type=str,
        help="Path to the boundary file (shp/GeoJSON) or a geocode representing the boundary.",
    )
    parser.add_argument(
        "out_dir",
        type=Path,
        help="Path to the output directory where the trimmed network will be saved.",
    )
    parser.add_argument(
        "--out_prefix",
        type=str,
        default="",
        help="Prefix for the output file name.",
    )
    parser.add_argument(
        "--out_format",
        type=str,
        choices=["geojson", "parquet"],
        default="geojson",
        help="Output file format (e.g., 'parquet', 'geojson').",
    )
    parser.add_argument(
        "-o",
        "--overwrite",
        action="store_true",
        help="Overwrite output files if they exist.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Increase verbosity of the output."
    )
    parser.add_argument(
        "-q", "--quiet", action="store_true", help="Decrease verbosity of the output."
    )

    args = parser.parse_args()
    WranglerLogger.debug(args)

    if args.verbose:
        log_level = logging.DEBUG
    elif args.quiet:
        log_level = logging.WARNING
    else:
        log_level = logging.INFO

    boundary_file = None
    boundary_geocode = None
    _file_suffix = [".shp", ".geojson", ".parquet"]
    if any(args.boundary.endswith(s) for s in _file_suffix):
        boundary_file = Path(args.boundary)
    else:
        boundary_geocode = args.boundary

    net = load_roadway_from_dir(args.network_path)
    WranglerLogger.info(f"Loaded roadway network from {args.network_path}")
    clipped_net = clip_roadway(net, boundary_file=boundary_file, boundary_geocode=boundary_geocode)
    write_roadway(
        clipped_net,
        prefix=args.out_prefix,
        out_dir=args.out_dir,
        file_format=args.out_format,
        overwrite=args.overwrite,
    )
    readme_txt = f"{args.out_prefix} Network was clipped using clip_roadway.py.\n\
        Date: {datetime.datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}\
        Args: {args}"
    with Path(args.out_dir / f"{args.out_prefix}README.txt").open("w") as f:
        f.write(readme_txt)
    WranglerLogger.info(f"Clipped network saved to {args.out_dir}")
