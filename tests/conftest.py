from pathlib import Path

import pandas as pd
import pytest

from network_wrangler import load_roadway

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 50000)


@pytest.fixture(scope="session", autouse=True)
def _test_logging(test_out_dir):
    from network_wrangler import setup_logging

    setup_logging(
        info_log_filename=test_out_dir / "tests.info.log",
        debug_log_filename=test_out_dir / "tests.debug.log",
    )


@pytest.fixture(scope="session")
def base_dir():
    return Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def bin_dir(base_dir):
    return base_dir / "bin"


@pytest.fixture(scope="session")
def example_dir(base_dir):
    return Path(base_dir) / "examples"


@pytest.fixture(scope="session")
def example_dirnames(example_dir):
    """List of directories in example_dir."""
    return list(example_dir.iterdir())


@pytest.fixture
def example_dirname(request, example_dirnames):
    return example_dirnames[request.param]


@pytest.fixture(scope="session")
def test_dir():
    return Path(__file__).resolve().parent


@pytest.fixture(scope="session")
def test_out_dir(test_dir):
    _test_out_dir = Path(test_dir) / "out"

    if not _test_out_dir.exists():
        _test_out_dir.mkdir()

    return _test_out_dir


@pytest.fixture(scope="session", autouse=True)
def _clear_out_dir(test_out_dir):
    import shutil

    for item in test_out_dir.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()


@pytest.fixture(scope="session")
def stpaul_card_dir(stpaul_ex_dir):
    return Path(stpaul_ex_dir) / "project_cards"


@pytest.fixture(scope="session")
def stpaul_ex_dir(example_dir):
    return Path(example_dir) / "stpaul"


@pytest.fixture(scope="session")
def small_ex_dir(example_dir):
    return Path(example_dir) / "small"


@pytest.fixture(scope="module")
def stpaul_net(stpaul_ex_dir):
    shape_filename = stpaul_ex_dir / "shape.geojson"
    link_filename = stpaul_ex_dir / "link.json"
    node_filename = stpaul_ex_dir / "node.geojson"

    return load_roadway(
        links_file=link_filename,
        nodes_file=node_filename,
        shapes_file=shape_filename,
    )


@pytest.fixture(scope="module")
def stpaul_transit_net(stpaul_ex_dir):
    from network_wrangler import load_transit

    return load_transit(stpaul_ex_dir)


@pytest.fixture(scope="module")
def small_net(small_ex_dir):
    from network_wrangler import load_roadway_from_dir

    return load_roadway_from_dir(small_ex_dir)


@pytest.fixture(scope="module")
def small_transit_net(small_ex_dir):
    from network_wrangler import load_transit

    return load_transit(small_ex_dir)


@pytest.fixture(scope="session")
def bad_project_cards(test_dir):
    """Card files which should fail."""
    bad_card_dir = Path(test_dir) / "data" / "project_cards_fail"
    return list(bad_card_dir.iterdir())
