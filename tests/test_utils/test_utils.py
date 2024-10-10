"""Tests for /utils.

Run just these tests using `pytest tests/test_utils/test_utils.py`
"""

import pytest
from shapely.geometry import LineString

from network_wrangler import WranglerLogger

slug_test_list = [
    {"text": "I am a roadway", "delim": "_", "answer": "i_am_a_roadway"},
    {"text": "I'm a roadway", "delim": "_", "answer": "im_a_roadway"},
    {"text": "I am a roadway", "delim": "-", "answer": "i-am-a-roadway"},
    {"text": "I am a roadway", "delim": "", "answer": "iamaroadway"},
]


@pytest.mark.parametrize("slug_test", slug_test_list)
def test_get_slug(request, slug_test):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    from network_wrangler.utils.utils import make_slug

    slug = make_slug(slug_test["text"], delimiter=slug_test["delim"])

    WranglerLogger.debug("From: {} \nTo: {}".format(slug_test["text"], slug))
    WranglerLogger.debug("Expected: {}".format(slug_test["answer"]))
    assert slug == slug_test["answer"]
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_get_unique_shape_id(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    from network_wrangler.roadway.utils import create_unique_shape_id

    geometry = LineString([[-93.0855338, 44.9662078], [-93.0843092, 44.9656997]])

    shape_id = create_unique_shape_id(geometry)

    assert shape_id == "72ceb24e2c632c02f7eae5e33ed12702"

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_point_from_xy(request):
    from numpy.testing import assert_almost_equal

    from network_wrangler.utils.geo import point_from_xy

    WranglerLogger.info(f"--Starting: {request.node.name}")
    in_xy = (871106.53, 316284.46)  # Minnesota Science Museum
    xy_crs = 26993  # Minnesota State Plane South, Meter
    out_crs = 4269  # https://epsg.io/4269

    out_point = point_from_xy(*in_xy, xy_crs=xy_crs, point_crs=out_crs)
    out_xy = (out_point.x, out_point.y)
    wgs_xy_science_museum = (-93.099, 44.943)

    assert_almost_equal(out_xy, wgs_xy_science_museum, decimal=2)
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_get_overlapping_range(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    from network_wrangler.utils.utils import get_overlapping_range

    a = range(5)
    b = range(5, 10)
    assert get_overlapping_range([a, b]) is None

    c = range(100, 106)
    assert get_overlapping_range([a, b, c]) is None

    i = (1, 5)
    j = (2, 7)
    assert get_overlapping_range([i, j]) == range(2, 5)

    k = range(3, 5)
    assert get_overlapping_range([i, j, k]) == range(3, 5)

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_all_list_elements_subset_of_single_element(request):
    from network_wrangler.utils.utils import list_elements_subset_of_single_element

    WranglerLogger.info(f"--Started: {request.node.name}")
    mixed_list = ["a", "b", ["a", "b", "c"]]
    assert list_elements_subset_of_single_element(mixed_list) is True

    mixed_list = ["a", "b", ["a", "b", "c"], "d"]
    assert list_elements_subset_of_single_element(mixed_list) is False

    mixed_list = ["a", "b", ["a", "b", "c"], "d", ["e", "f"]]
    assert list_elements_subset_of_single_element(mixed_list) is False

    mixed_list = ["a", "b", "c", "d"]
    assert list_elements_subset_of_single_element(mixed_list) is False

    mixed_list = [["a", "b", "c"], ["a", "b", "c", "d"], ["a", "b", "c", "d", "e"]]
    assert list_elements_subset_of_single_element(mixed_list) is True

    mixed_list = [["a", "b", "c"], ["a", "b", "c", "d"], ["a", "b", "c", "d", "e"], "f"]
    assert list_elements_subset_of_single_element(mixed_list) is False

    mixed_list = []
    assert list_elements_subset_of_single_element(mixed_list) is False

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_check_one_or_one_superset_present(request):
    from network_wrangler.utils.utils import check_one_or_one_superset_present

    WranglerLogger.info(f"--Starting: {request.node.name}")
    mixed_list = ["a", "b", ["a", "b", "c"]]
    field_list = ["a", "b", "c"]
    assert check_one_or_one_superset_present(mixed_list, field_list) is True

    field_list = ["a", "b"]
    assert check_one_or_one_superset_present(mixed_list, field_list) is False

    field_list = ["e", "f"]
    mixed_list = ["a", "b", ["a", "b", "c"], "d", ["e", "f"]]
    assert check_one_or_one_superset_present(mixed_list, field_list) is True

    field_list = ["b"]
    mixed_list = ["a", "b", "c", "d"]
    assert check_one_or_one_superset_present(mixed_list, field_list) is True

    field_list = ["a", "b"]
    mixed_list = ["a", "b", "c", "d"]
    assert check_one_or_one_superset_present(mixed_list, field_list) is False

    field_list = ["a", "b", "c", "d"]
    mixed_list = [["a", "b", "c"], ["a", "b", "c", "d"], ["a", "b", "c", "d", "e"]]
    assert check_one_or_one_superset_present(mixed_list, field_list) is True

    field_list = ["a", "b", "c", "e"]
    mixed_list = [["a", "b", "c"], ["a", "b", "c", "d"], ["a", "b", "c", "d", "e"]]
    assert check_one_or_one_superset_present(mixed_list, field_list) is True

    field_list = ["a", "b"]
    mixed_list = ["b", "c", "d"]
    assert check_one_or_one_superset_present(mixed_list, field_list) is True

    field_list = ["a", "e"]
    mixed_list = ["b", "c", "d"]
    assert check_one_or_one_superset_present(mixed_list, field_list) is False
    WranglerLogger.info(f"--Finished: {request.node.name}")
