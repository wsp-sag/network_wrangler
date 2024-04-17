from .io import load_roadway_from_dir, write_roadway
from ..logger import WranglerLogger


def convert_roadway(
    input_path,
    output_format,
    out_dir,
    input_suffix,
    out_prefix,
    overwrite,
):
    if input_suffix is None:
        input_suffix = "geojson"
    WranglerLogger.info(
        f"Loading roadway network from {input_path} with suffix {input_suffix}"
    )
    net = load_roadway_from_dir(input_path, suffix=input_suffix)
    WranglerLogger.info(
        f"Writing roadway network to {out_dir} in {output_format} format."
    )
    write_roadway(
        net,
        prefix=out_prefix,
        out_dir=out_dir,
        file_format=output_format,
        overwrite=overwrite,
    )
