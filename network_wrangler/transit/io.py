from pathlib import Path
from typing import Union

import pandas as pd

from ..transitnetwork import TransitNetwork
from ..transit.feed import Feed
from ..logger import WranglerLogger
from ..utils import unzip_file

FEED_TABLE_READ = ["frequencies", "routes", "shapes", "stop_times", "stops", "trips"]


def _feed_path_ref(path: Path) -> Path:
    if path.suffix == ".zip":
        path = unzip_file(path)
    if not path.is_dir:
        raise FileExistsError(f"Feed cannot be found at: {path}")

    return path


def load_feed_from_path(feed_path: Union[Path, str], suffix: str = "txt") -> Feed:
    """
    Create a TransitNetwork object from the path to a GTFS transit feed.

    Args:
        feed_path (Union[Path, str]): The path to the GTFS transit feed.
        suffix: the suffix of the files to read. Defaults to "txt"

    Returns:
        Feed: The TransitNetwork object created from the GTFS transit feed.
    """
    feed_path = _feed_path_ref(feed_path)  # unzips if needs to be unzipped
    WranglerLogger.info(f"Reading GTFS feed tables from {feed_path}")

    feed_files = {
        table: next(feed_path.glob(f"*{table}.{suffix}")) for table in FEED_TABLE_READ
    }

    feed_dfs = {
        table: _read_table_from_file(table, file) for table, file in feed_files.items()
    }

    return Feed(feed_dfs)


def _read_table_from_file(table: str, file: Path) -> pd.DataFrame:
    WranglerLogger.debug(f"...reading {file}.")
    if file.suffix in [".csv", ".txt"]:
        return pd.read_csv(file)
    elif file.suffix == ".parquet":
        return pd.read_parquet(file)


def load_feed_from_dfs(feed_dfs: dict) -> Feed:
    """
    Create a TransitNetwork object from a dictionary of DataFrames representing a GTFS transit feed.

    Args:
        feed_dfs (dict): A dictionary containing DataFrames representing the tables of a GTFS transit feed.

    Returns:
        Feed: A Feed object representing the transit network.

    Raises:
        ValueError: If the feed_dfs dictionary does not contain all the required tables.

    Example:
        >>> feed_dfs = {
        ...     'agency': agency_df,
        ...     'routes': routes_df,
        ...     'stops': stops_df,
        ...     'trips': trips_df,
        ...     'stop_times': stop_times_df
        ... }
        >>> feed = load_feed_from_dfs(feed_dfs)
    """
    if not all([table in feed_dfs for table in FEED_TABLE_READ]):
        raise ValueError(
            f"feed_dfs must contain the following tables: {FEED_TABLE_READ}"
        )
    return Feed(feed_dfs)


def load_transit(
    feed: Union[Feed, dict[str, pd.DataFrame], str, Path],
    suffix: str = "txt",
) -> TransitNetwork:
    """
    Create a TransitNetwork object.

    This function takes in a `feed` parameter, which can be one of the following types:
    - `Feed`: A Feed object representing a transit feed.
    - `dict[str, pd.DataFrame]`: A dictionary of DataFrames representing transit data.
    - `str` or `Path`: A string or a Path object representing the path to a transit feed file.

    Args:
        feed: Feed boject, dict of transit data frames, or path to transit feed data
        suffix: the suffix of the files to read. Defaults to "txt"

    Returns:
    A TransitNetwork object representing the loaded transit network.

    Raises:
    ValueError: If the `feed` parameter is not one of the supported types.

    Example usage:
    ```
    transit_network_from_zip = load_transit("path/to/gtfs.zip")

    transit_network_from_unzipped_dir = load_transit("path/to/files")

    transit_network_from_parquet = load_transit("path/to/files", suffix="parquet")

    dfs_of_transit_data = {"routes": routes_df, "stops": stops_df, "trips": trips_df...}
    transit_network_from_dfs = load_transit(dfs_of_transit_data)
    ```

    """
    if feed is Feed:
        return TransitNetwork(feed)
    elif feed is dict:
        return TransitNetwork(load_feed_from_dfs(feed))
    elif feed is str or Path:
        return TransitNetwork(load_feed_from_path(feed, suffix=suffix))
    else:
        raise ValueError(
            "TransitNetwork must be seeded with a Feed, dict of dfs or Path"
        )


def write_transit(
    transit_net,
    out_dir: Union[Path, str] = ".",
    prefix: Union[Path, str] = None,
    format: str = "csv",
    overwrite: bool = True,
) -> None:
    """
    Writes a network in the transit network standard

    Args:
        transit_net: a TransitNetwork instance
        out_dir: directory to write the network to
        format: the format of the output files. Defaults to "csv" which will actually be written to suffix of .txt.
        prefix: prefix to add to the file name
        overwrite: if True, will overwrite the files if they already exist. Defaults to True
    """
    out_dir = Path(out_dir)
    if not out_dir.is_dir():
        if out_dir.parent.is_dir():
            out_dir.mkdir()
        else:
            raise FileNotFoundError(
                f"Output directory {out_dir} ands its parent path does not exist"
            )

    prefix = f"_{prefix}" if prefix else ""
    format = "txt" if format is "csv" else format  # because GTFS is weird...

    _feed = transit_net.feed
    for table in _feed.tables:
        df = _feed.__dict__[table]
        outpath = out_dir / f"{prefix}{table}.{format}"

        if outpath.exists() and not overwrite:
            raise FileExistsError(
                f"File {outpath} already exists and overwrite set to false."
            )
        if format == "csv":
            outpath = outpath.with_suffix(".csv")
            df.to_csv(outpath, index=False, date_format="%H:%M:%S")
        elif format == "parquet":
            df.to_parquet(outpath)
    WranglerLogger.info(f"Wrote {len(_feed.tables)} files to {out_dir}")
