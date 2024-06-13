"""Helper functions for reading and writing files to reduce boilerplate."""

import tempfile
import shutil
import weakref

from pathlib import Path
from typing import Union

import geopandas as gpd
import pandas as pd

from ..params import EST_PD_READ_SPEED

from ..logger import WranglerLogger
from .time import format_time

try:
    gpd.options.io_engine = "pyogrio"
except:  # noqa: E722
    if gpd.__version__ < "0.12.0":
        WranglerLogger.warning(
            f"Installed Geopandas version {gpd.__version__} isn't recent enough to support\
                pyogrio. Falling back to default engine (fiona).\
                Update geopandas and install pyogrio to benefit."
        )
    else:
        WranglerLogger.warning(
            "Geopandas is not using pyogrio as the I/O engine.\
                Install pyogrio to benefit from faster I/O."
        )


class FileReadError(Exception):
    """Raised when there is an error reading a file."""

    pass


class FileWriteError(Exception):
    """Raised when there is an error writing a file."""

    pass


def write_table(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    filename: Path,
    overwrite: bool = False,
    **kwargs,
) -> None:
    """Write a dataframe or geodataframe to a file.

    Args:
        df (pd.DataFrame): dataframe to write.
        filename (Path): filename to write to.
        overwrite (bool): whether to overwrite the file if it exists. Defaults to False.
        kwargs: additional arguments to pass to the writer.

    """
    filename = Path(filename)
    if filename.exists() and not overwrite:
        raise FileExistsError(f"File {filename} already exists and overwrite is False.")

    if filename.parent.is_dir() and not filename.parent.exists():
        filename.parent.mkdir(parents=True)

    WranglerLogger.debug(f"Writing to {filename}.")

    if "shp" in filename.suffix:
        df.to_file(filename, index=False, **kwargs)
    elif "parquet" in filename.suffix:
        df.to_parquet(filename, index=False, **kwargs)
    elif "csv" in filename.suffix:
        df.to_csv(filename, index=False, date_format="%H:%M:%S", **kwargs)
    elif "txt" in filename.suffix:
        df.to_csv(filename, index=False, date_format="%H:%M:%S", **kwargs)
    elif "geojson" in filename.suffix:
        # required due to issues with list-like columns
        if isinstance(df, gpd.GeoDataFrame):
            data = df.to_json(drop_id=True)
        else:
            data = df.to_json(orient="records", index=False)
        with open(filename, "w", encoding="utf-8") as file:
            file.write(data)
    elif "json" in filename.suffix:
        with open(filename, "w") as f:
            f.write(df.to_json(orient="records"))
    else:
        raise NotImplementedError(f"Filetype {filename.suffix} not implemented.")


def _estimate_read_time_of_file(filepath: Union[str, Path]):
    """Estimates read time in seconds based on a given file size and speed factor.

    The speed factor is MB per second which you can adjust based on empirical data.

    TODO: implement based on file type and emirical
    """
    # MB per second
    filepath = Path(filepath)
    file_size_mb = filepath.stat().st_size / (1024 * 1024)  # Convert bytes to MB
    filetype = filepath.suffix[1:]
    if filetype in EST_PD_READ_SPEED:
        return format_time(file_size_mb * EST_PD_READ_SPEED[filetype])
    else:
        return "unknown"


def read_table(filename: Path, sub_filename: str = None) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Read file and return a dataframe or geodataframe.

    If filename is a zip file, will unzip to a temporary directory.

    NOTE:  if you are accessing multiple files from this zip file you will want to unzip it first
    and THEN access the table files so you don't create multiple duplicate unzipped tmp dirs.

    Args:
        filename (Path): filename to load.
        sub_filename: if the file is a zip, the sub_filename to load.

    """
    filename = Path(filename)
    if filename.suffix == ".zip":
        filename = unzip_file(filename) / sub_filename
    WranglerLogger.debug(f"Estimated read time: {_estimate_read_time_of_file(filename)}.")
    if any([x in filename.suffix for x in ["geojson", "shp", "csv"]]):
        try:
            return gpd.read_file(filename)
        except:  # noqa: E722
            if "csv" in filename.suffix:
                return pd.read_csv(filename)
            raise FileReadError
    elif "parquet" in filename.suffix:
        try:
            return gpd.read_parquet(filename)
        except:  # noqa: E722
            return pd.read_parquet(filename)
    elif "json" in filename.suffix:
        with open(filename) as f:
            return pd.read_json(f, orient="records")
    raise NotImplementedError(f"Filetype {filename.suffix} not implemented.")


def unzip_file(path: Path) -> Path:
    """Unzips a file to a temporary directory and returns the directory path."""
    tmpdir = tempfile.mkdtemp()
    shutil.unpack_archive(path, tmpdir)

    def finalize() -> None:
        shutil.rmtree(tmpdir)

    # Lazy cleanup
    weakref.finalize(tmpdir, finalize)

    return tmpdir
