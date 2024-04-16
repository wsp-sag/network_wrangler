import pytest
import pandas as pd
from pandas import testing as tm
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
    print("\n--Starting:", request.node.name)

    from network_wrangler.utils import make_slug

    slug = make_slug(slug_test["text"], delimiter=slug_test["delim"])

    print("From: {} \nTo: {}".format(slug_test["text"], slug))
    print("Expected: {}".format(slug_test["answer"]))
    assert slug == slug_test["answer"]


def test_time_convert(request):
    print("\n--Starting:", request.node.name)

    time_tests = [
        (("00:00:00", "00:00:10"), (0, 10)),
        (("0:00", "0:10:00"), (0, 600)),
        (("01:02:03", "01:02:23"), (3723, 3743)),
        (("1:02", "1:02:13"), (3720, 3733)),
        (("25:24:23", "25:24:33"), (91463, 91473)),
        (("250:24:23", "250:24:25"), (901463, 901465)),
    ]

    from pandas import DataFrame

    df = DataFrame(time_tests, columns=["time", "time_results"])
    print("Original Time Series", df)

    from network_wrangler.utils.time import parse_timespans_to_secs

    df["time"] = df["time"].apply(parse_timespans_to_secs)
    print("Result Time Series", df)
    from pandas.testing import assert_series_equal

    assert_series_equal(df["time"], df["time_results"], check_names=False)


def test_get_distance_bw_lat_lon(request):
    print("\n--Starting:", request.node.name)
    from network_wrangler.utils import haversine_distance

    start = [-93.0889873, 44.966861]
    end = [-93.08844310000001, 44.9717832]
    dist = haversine_distance(start, end)
    print(dist)
    assert dist == 0.34151200885686445
    print("--Finished:", request.node.name)


def test_get_unique_shape_id(request):
    print("\n--Starting:", request.node.name)
    from network_wrangler.roadway.utils import create_unique_shape_id

    geometry = LineString([[-93.0855338, 44.9662078], [-93.0843092, 44.9656997]])

    shape_id = create_unique_shape_id(geometry)

    assert shape_id == "72ceb24e2c632c02f7eae5e33ed12702"

    print("--Finished:", request.node.name)


def test_point_from_xy(request):
    from network_wrangler.utils import point_from_xy
    from numpy.testing import assert_almost_equal

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
    from network_wrangler.utils import get_overlapping_range

    WranglerLogger.info(f"--Starting: {request.node.name}")

    a = range(0, 5)
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


SPLT_DF_TEST_PARAMS = [
    (
        [1, 2, 3, 4, 5],
        [1, 2],
        ([], [1, 2], [3, 4, 5]),
    ),
    (
        [1, 2, 3, 4, 5],
        [0, 2],
        ([], [1, 2], [3, 4, 5]),
    ),
    (
        [1, 2, 3, 4, 5],
        [1, 6],
        ValueError,
    ),
    (
        [1, 2, 3, 4, 5, 6, 7],
        [2, 5],
        ([1], [2, 3, 4, 5], [6, 7]),
    ),
    (
        [1, 2, 3, 4, 5],
        [5, 0],
        ([1, 2, 3, 4], [5], []),
    ),
    (
        [1, 2, 3, 2],
        [3, 2],
        ([1, 2], [3, 2], []),
    ),
    ([1, 2, 3, 2], [2, 1], ValueError),
]


@pytest.mark.parametrize(
    "ref_list, item_list, expected_result",
    SPLT_DF_TEST_PARAMS,
)
def test_segment_list_by_list(request, ref_list, item_list, expected_result):
    from network_wrangler.utils.data import segment_data_by_selection

    if expected_result in [ValueError]:
        with pytest.raises(expected_result):
            segment_data_by_selection(ref_list, item_list)
    else:
        calc_answer = segment_data_by_selection(item_list, ref_list)
        assert expected_result == calc_answer


def test_segment_series_by_list(request):
    from network_wrangler.utils.data import segment_data_by_selection

    s = pd.Series([1, 2, 3, 4, 5], dtype="int64")
    item_list = [1, 2]
    exp_answer = (
        pd.Series([], dtype="int64"),
        pd.Series([1, 2], dtype="int64"),
        pd.Series([3, 4, 5], dtype="int64"),
    )

    calc_answer = segment_data_by_selection(item_list, s)
    for calc, exp in zip(calc_answer, exp_answer):
        WranglerLogger.debug(f"\ncalc:\n{calc}")
        WranglerLogger.debug(f"\nexp:\n{exp}")
        tm.assert_series_equal(calc, exp)


def test_segment_df_by_list(request):
    from network_wrangler.utils.data import segment_data_by_selection

    s = pd.DataFrame({"mynodes": [1, 2, 3, 4, 3, 2, 5]})
    item_list = [2, 3]
    exp_answer = ([1], [2, 3, 4, 3], [2, 5])

    calc_answer = segment_data_by_selection(item_list, s, field="mynodes")
    for calc, exp in zip(calc_answer, exp_answer):
        # WranglerLogger.debug(f"\ncalc:\n{calc['mynodes']}")
        # WranglerLogger.debug(f"\nexp:\n{exp}")
        assert exp == calc["mynodes"].to_list()
