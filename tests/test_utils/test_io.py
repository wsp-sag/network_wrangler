"""Module for testing the utils.io module."""

import pytest

from network_wrangler import WranglerLogger
from network_wrangler.utils.data import diff_dfs
from network_wrangler.utils.io_table import convert_file_serialization
from network_wrangler.utils.time import str_to_seconds_from_midnight


@pytest.mark.skip(reason="Not implemented")
def test_convert_in_chunks(example_dir, tmpdir):
    convert_file_serialization(
        example_dir / "stpaul" / "link.json", tmpdir / "chunked_links.parquet", chunk_size=100
    )
    convert_file_serialization(
        example_dir / "stpaul" / "link.json", tmpdir / "not_chunked_links.parquet"
    )
    # make sure they are not empty
    assert (tmpdir / "chunked_links.parquet").stat().st_size > 0
    assert (tmpdir / "not_chunked_links.parquet").stat().st_size > 0

    # make sure they are the same size
    assert (tmpdir / "chunked_links.parquet").stat().st_size == (
        tmpdir / "not_chunked_links.parquet"
    ).stat().st_size

    # make sure they are the same
    assert (tmpdir / "chunked_links.parquet").read_bytes() == (
        tmpdir / "not_chunked_links.parquet"
    ).read_bytes()

    # clean up
    (tmpdir / "chunked_links.parquet").unlink()
    (tmpdir / "not_chunked_links.parquet").unlink()


v0_links_json = [
    {
        "A": 1,
        "B": 2,
        "model_link_id": 1,
        "name": "Main St",
        "lanes": 1,
    },
    {
        "A": 2,
        "B": 3,
        "model_link_id": 2,
        "name": "Main St",
        "lanes": {
            "default": 4,
            "timeofday": [
                {
                    "time": (
                        str_to_seconds_from_midnight("06:00"),
                        str_to_seconds_from_midnight("10:00"),
                    ),
                    "value": 2,
                },
                {
                    "time": (
                        str_to_seconds_from_midnight("12:00"),
                        str_to_seconds_from_midnight("14:00"),
                    ),
                    "category": ["hov2"],
                    "value": 2,
                },
            ],
        },
    },
]

v1_links_json = [
    {
        "A": 1,
        "B": 2,
        "model_link_id": 1,
        "name": "Main St",
        "lanes": 1,
    },
    {
        "A": 2,
        "B": 3,
        "model_link_id": 2,
        "name": "Main St",
        "lanes": 4,
        "sc_lanes": [
            {"timespan": ["06:00", "10:00"], "value": 2},
            {"timespan": ["12:00", "14:00"], "category": "hov2", "value": 2},
        ],
    },
]

nodes_json = [
    {"model_node_id": 1, "X": 0, "Y": 0},
    {"model_node_id": 2, "X": 1, "Y": 1},
    {"model_node_id": 3, "X": 2, "Y": 2},
]


def test_read_in_v0_links():
    from network_wrangler.roadway.links.create import data_to_links_df
    from network_wrangler.roadway.nodes.create import data_to_nodes_df

    nodes_df = data_to_nodes_df(nodes_json)
    links_v0_df = data_to_links_df(v0_links_json, nodes_df=nodes_df)
    links_v1_df = data_to_links_df(v1_links_json, nodes_df=nodes_df)
    different_df = diff_dfs(links_v0_df, links_v1_df)
    assert not different_df


def test_convert_v1_v0_links():
    from pandas import DataFrame

    from network_wrangler.models.roadway.converters import translate_links_df_v1_to_v0
    from network_wrangler.roadway.links.create import data_to_links_df
    from network_wrangler.roadway.nodes.create import data_to_nodes_df

    nodes_df = data_to_nodes_df(nodes_json)
    links_v0_df = DataFrame(v0_links_json)
    links_df = data_to_links_df(v0_links_json, nodes_df=nodes_df)
    links_converted_v0_df = translate_links_df_v1_to_v0(links_df).reset_index(drop=True)
    val_v0 = links_v0_df.loc[1, "lanes"]
    val_converted_v1 = links_converted_v0_df.loc[1, "lanes"]
    assert val_v0["default"] == val_converted_v1["default"]
    assert val_v0["timeofday"] == val_converted_v1["timeofday"]
