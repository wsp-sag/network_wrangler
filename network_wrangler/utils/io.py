import tempfile
import shutil
import weakref
from pathlib import Path

from typing import Union

import geopandas as gpd
import pandas as pd

from ..logger import WranglerLogger

try:
    gpd.options.io_engine = "pyogrio"
except:
    WranglerLogger.warning(
        "pyogrio not installed, falling back to default engine (fiona)"
    )


class FileReadError(Exception):
    pass


class FileWriteError(Exception):
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
        overwriter (bool): whether to overwrite the file if it exists. Defaults to False.
        kwargs: additional arguments to pass to the writer.

    """
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
        df.to_csv(filename, index=False, **kwargs)
    elif "geojson" in filename.suffix:
        # required due to issues with list-like columns
        data = df.to_json(drop_id=True)
        with open(filename, "w", encoding="utf-8") as file:
            file.write(data)
    elif "json" in filename.suffix:
        with open(filename, "w") as f:
            f.write(df.to_json(orient="records"))
    else:
        raise NotImplementedError(f"Filetype {filename.suffix} not implemented.")


def read_table(
    filename: Path, sub_filename: str = None
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
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

    if any([x in filename.suffix for x in ["geojson", "shp", "csv"]]):
        try:
            return gpd.read_file(filename)
        except:
            if "csv" in filename.suffix:
                return pd.read_csv(filename)
            raise FileReadError
    elif "parquet" in filename.suffix:
        try:
            return gpd.read_parquet(filename)
        except:
            return pd.read_parquet(filename)
    elif "json" in filename.suffix:
        with open(filename) as f:
            return pd.read_json(f, orient="records")
    raise NotImplementedError(f"Filetype {filename.suffix} not implemented.")


def unzip_file(path: Path) -> Path:
    tmpdir = tempfile.mkdtemp()
    shutil.unpack_archive(path, tmpdir)

    def finalize() -> None:
        shutil.rmtree(tmpdir)

    # Lazy cleanup
    weakref.finalize(tmpdir, finalize)

    return tmpdir
