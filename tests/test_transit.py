import os
import json
import pytest
from network_wrangler import TransitNetwork, setupLogging


"""
Run just the tests labeled transit using `pytest -v -m transit`
"""

STPAUL_DIR = os.path.join(os.getcwd(), 'example', 'stpaul')
SCRATCH_DIR = os.path.join(os.getcwd(), 'tests')


@pytest.mark.basic
@pytest.mark.travis
@pytest.mark.transit
def test_transit_read_write(request):
    print("\n--Starting:", request.node.name)
    transit_net = TransitNetwork.read(feed_path=STPAUL_DIR)
    print('Transit Net Directory:', STPAUL_DIR)

    transit_net.write(outpath=SCRATCH_DIR)
    print('Transit Write Directory:', SCRATCH_DIR)

    print("--Finished:", request.node.name)


@pytest.mark.basic
@pytest.mark.transit
def test_select_transit_features(request):
    print("\n--Starting:", request.node.name)
    net = TransitNetwork.read(STPAUL_DIR)

    test_selections = {
        "1. simple trip_id": {
            'trip_id': '14940701-JUN19-MVS-BUS-Weekday-01',
            'answer': ['14940701-JUN19-MVS-BUS-Weekday-01']
        },
        "2. multiple trip_id": {
            'trip_id': [
                '14940975-JUN19-MVS-BUS-Weekday-01',  # unordered
                '14940701-JUN19-MVS-BUS-Weekday-01',
            ],
            'answer': [
                '14940701-JUN19-MVS-BUS-Weekday-01',
                '14940975-JUN19-MVS-BUS-Weekday-01'
            ]
        }
    }

    for i, sel in test_selections.items():
        print("--->", i, "\n", sel)
        selected_trips = net.select_transit_features(sel)
        assert(set(selected_trips) == set(sel['answer']))

    print("--Finished:", request.node.name)


if __name__ == '__main__':
    test_transit_read_write()
