import os
import pytest

slug_test_list = [
    {"text": "I am a roadway", "delim": "_", "answer": "i_am_a_roadway"},
    {"text": "I'm a roadway", "delim": "_", "answer": "im_a_roadway"},
    {"text": "I am a roadway", "delim": "-", "answer": "i-am-a-roadway"},
    {"text": "I am a roadway", "delim": "", "answer": "iamaroadway"},
]


@pytest.mark.parametrize("slug_test", slug_test_list)
def test_get_slug(request, slug_test):
    print("\n--Starting:", request.node.name)
    from network_wrangler.Utils import make_slug

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

    from network_wrangler.Utils import parse_time_spans

    df["time"] = df["time"].apply(parse_time_spans)
    print("Result Time Series", df)
    from pandas.testing import assert_series_equal

    assert_series_equal(df["time"], df["time_results"], check_names=False)


null_val_type_list = [("1", 0), ("3.2", 0.0), ("Howdy", ""), ("False", False)]
