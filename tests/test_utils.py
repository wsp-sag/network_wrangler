import os
import pytest


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

@pytest.mark.geography
@pytest.mark.travis
def test_get_distance_bw_lat_lon(request):
    print("\n--Starting:", request.node.name)

    start = [-93.0889873, 44.966861]
    end = [-93.08844310000001, 44.9717832]

    from network_wrangler import haversine_distance
    dist = haversine_distance(start, end)
    print(dist)

    print("--Finished:", request.node.name)

@pytest.mark.geography
@pytest.mark.travis
def test_lat_lon_offset(request):
    print("\n--Starting:", request.node.name)

    in_lat_lon = [-93.0903549, 44.961085]
    print(in_lat_lon)
    from network_wrangler import offset_lat_lon
    new_lat_lon = offset_lat_lon(in_lat_lon)
    print(new_lat_lon)

    print("--Finished:", request.node.name)


@pytest.mark.travis
def test_get_unique_shape_id(request):
    print("\n--Starting:", request.node.name)

    from shapely.geometry import LineString
    geometry = LineString([[-93.0855338, 44.9662078], [-93.0843092, 44.9656997]])

    from network_wrangler import create_unique_shape_id
    shape_id = create_unique_shape_id(geometry)

    assert shape_id == "72ceb24e2c632c02f7eae5e33ed12702"

    print("--Finished:", request.node.name)

@pytest.mark.elo
@pytest.mark.travis
def test_link_df_to_json(request):
    print("\n--Starting:", request.node.name)
    json_in = [{"a":1,"b":2,"distance":5.1},{"a":2,"b":3,"distance":1.2}]

    from pandas import DataFrame
    df = DataFrame(json_in)

    from network_wrangler import link_df_to_json

    json_out = link_df_to_json(df,["a","b","distance"])

    assert(json_in == json_out)
