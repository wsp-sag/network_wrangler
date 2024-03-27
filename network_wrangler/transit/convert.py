from .io import load_transit, write_transit
from ..logger import WranglerLogger


def convert_transit(
    input_path,
    output_format,
    out_dir,
    input_suffix,
    out_prefix,
    overwrite,
):
    if input_suffix is None:
        input_suffix = "csv"
    WranglerLogger.info(
        f"Loading transit net from {input_path} with input type {input_suffix}"
    )
    net = load_transit(input_path, suffix=input_suffix)
    WranglerLogger.info(
        f"Writing transit network to {out_dir} in {output_format} format."
    )
    write_transit(
        net,
        prefix=out_prefix,
        out_dir=out_dir,
        format=output_format,
        overwrite=overwrite,
    )
