import os
import pytest

from shapely.geometry import LineString

from network_wrangler import haversine_distance
from network_wrangler import create_unique_shape_id
from network_wrangler import offset_location_reference

slug_test_list = [
    {"text": "I am a roadway", "delim": "_", "answer": "i_am_a_roadway"},
    {"text": "I'm a roadway", "delim": "_", "answer": "im_a_roadway"},
    {"text": "I am a roadway", "delim": "-", "answer": "i-am-a-roadway"},
    {"text": "I am a roadway", "delim": "", "answer": "iamaroadway"},
]


@pytest.mark.travis
@pytest.mark.parametrize("slug_test", slug_test_list)
def test_get_slug(request, slug_test):
    print("\n--Starting:", request.node.name)

    from network_wrangler.utils import make_slug

    slug = make_slug(slug_test["text"], delimiter=slug_test["delim"])

    print("From: {} \nTo: {}".format(slug_test["text"], slug))
    print("Expected: {}".format(slug_test["answer"]))
    assert slug == slug_test["answer"]


@pytest.mark.travis
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

    from network_wrangler.utils import parse_time_spans

    df["time"] = df["time"].apply(parse_time_spans)
    print("Result Time Series", df)
    from pandas.testing import assert_series_equal

    assert_series_equal(df["time"], df["time_results"], check_names=False)


@pytest.mark.get_dist
@pytest.mark.travis
def test_get_distance_bw_lat_lon(request):
    print("\n--Starting:", request.node.name)

    start = [-93.0889873, 44.966861]
    end = [-93.08844310000001, 44.9717832]
    dist = haversine_distance(start, end)
    print(dist)
    assert dist == 0.34151200885686445
    print("--Finished:", request.node.name)


@pytest.mark.test_hash
@pytest.mark.roadway
def test_get_unique_shape_id(request):
    geometry = LineString([[-93.0855338, 44.9662078], [-93.0843092, 44.9656997]])

    shape_id = create_unique_shape_id(geometry)

    assert shape_id == "72ceb24e2c632c02f7eae5e33ed12702"

    print("--Finished:", request.node.name)


@pytest.mark.travis
def test_location_reference_offset(request):
    print("\n--Starting:", request.node.name)

    location_reference = [
        {"sequence": 1, "point": [-93.0903549, 44.961085]},
        {"sequence": 2, "point": [-93.0889873, 44.966861]},
    ]

    print("original ref", location_reference)

    expected_location_reference = [
        {"sequence": 1, "point": [-93.09022968479499, 44.961070179988084]},
        {"sequence": 2, "point": [-93.08886207218725, 44.966846179988075]},
    ]

    new_location_reference = offset_location_reference(location_reference)
    print("new ref", new_location_reference)

    assert new_location_reference == expected_location_reference

    print("--Finished:", request.node.name)
