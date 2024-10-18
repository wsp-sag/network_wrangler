#!/usr/bin/env python
"""Trims a transit network based on a given boundary and outputs the trimmed network.

Usage: python clip_transit.py <network_path> <boundary> <out_dir> [--out_prefix <prefix>]
    [--out_format <format>].

Arguments:
- network_path: Path to the input transit network directory.
- boundary: Path to the boundary file (shapefile or GeoJSON) or a geocode representing the \
    boundary (e.g. "Raleigh, NC, USA").
- out_dir: Path to the output directory where the trimmed network will be saved.
- --out_prefix <prefix>: Prefix for the output file name. (optional)
- --out_format <format>: Output file format. Supported formats: 'csv', 'parquet'. Default: 'csv'. \
    (optional)
- -o: Overwrite the output files if they already exist. Otherwise, will bork if files exist. \
    (optional)

Example usage:
python clip_transit.py /path/to/network_dir /path/to/boundary.shp /path/to/output_dir \
    --out_prefix clip --out_format geojson

"""

import argparse
import sys
from pathlib import Path

from network_wrangler import WranglerLogger
from network_wrangler.transit.clip import clip_transit
from network_wrangler.transit.io import load_transit, write_transit

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(
            description="Trim a transit network based on a given boundary.",
            usage="%(prog)s [options] <network_path> <boundary> <out_dir>",
        )
        parser.add_argument(
            "network_path",
            type=str,
            help="Path to the input transit network directory.",
        )
        parser.add_argument(
            "boundary",
            type=str,
            help="Path to the boundary file (shapefile, geoparquet or GeoJSON) or a geocode \
                representing the boundary.",
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
            choices=["csv", "parquet"],
            default="csv",
            help="Output file format (e.g., 'parquet', 'csv').",
        )
        parser.add_argument("-o", action="store_true")
        args = parser.parse_args()

        boundary_file = None
        boundary_geocode = None
        _file_suffix = [".shp", ".geojson", ".parquet"]
        if any(args.boundary.endswith(s) for s in _file_suffix):
            boundary_file = Path(args.boundary)
        else:
            boundary_geocode = args.boundary

        net = load_transit(args.network_path)
        clipped_net = clip_transit(
            net, boundary_file=boundary_file, boundary_geocode=boundary_geocode
        )
        write_transit(
            clipped_net,
            prefix=args.out_prefix,
            out_dir=args.out_dir,
            file_format=args.out_format,
            overwrite=args.o,
        )
    except Exception as e:
        WranglerLogger.error(f"clip_transit error: {e}")
        sys.exit(1)
