import os

import pytest

from network_wrangler import WranglerLogger
from projectcard import read_card

"""
Run just the tests using `pytest tests/test_transit/test_selections.py`
"""

TEST_SELECTIONS = [
        {
            "name":"1. simple trip_id", 
            "selection":{"trip_id": "14940701-JUN19-MVS-BUS-Weekday-01"},
            "answer": ["14940701-JUN19-MVS-BUS-Weekday-01"],
        },
        {
            "name": "2. multiple trip_id", 
            "selection":{"trip_id": [
                "14969841-JUN19-RAIL-Weekday-01",  # unordered
                "14940701-JUN19-MVS-BUS-Weekday-01",
            ]},
            "answer": [
                "14940701-JUN19-MVS-BUS-Weekday-01",
                "14969841-JUN19-RAIL-Weekday-01",
            ],
        },
        {
            "name": "3. route_id", 
            "selection":{"route_id": "365-111"},
            "answer": ["14947182-JUN19-MVS-BUS-Weekday-01"],
        },
]

@pytest.mark.parametrize("selection", TEST_SELECTIONS)
def test_select_transit_features(request,stpaul_transit_net, selection):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    WranglerLogger.info(f"     Name: {selection['name']}")
    WranglerLogger.debug(f"     Selection: {selection['selection']}")

    selected_trips = stpaul_transit_net.select_transit_features(selection["selection"])
    WranglerLogger.debug(f"    Exepected Answer: /n{selection['answer']}")
    assert set(selected_trips) == set(selection["answer"])

    WranglerLogger.info(f"--Finished: {request.node.name}")


TEST_CARD_SELECTIONS = [
    {
        "file": "7_simple_transit_attribute_change.yml",
        "answer": ["14940701-JUN19-MVS-BUS-Weekday-01"],
    },
    {
        "file": "7a_multi_transit_attribute_change.yml",
        "answer": [
            "14940701-JUN19-MVS-BUS-Weekday-01",
            "14948032-JUN19-MVS-BUS-Weekday-01",
        ],
    },
    {
        "file": "8_simple_transit_attribute_change.yml",
        "answer": [
            "14944012-JUN19-MVS-BUS-Weekday-01",
            "14944018-JUN19-MVS-BUS-Weekday-01",
            "14944019-JUN19-MVS-BUS-Weekday-01",
            "14944022-JUN19-MVS-BUS-Weekday-01",
        ],
    },
    {
        "file": "8a_multi_transit_attribute_change.yml",
        "answer": [
            "14944012-JUN19-MVS-BUS-Weekday-01",
            "14944018-JUN19-MVS-BUS-Weekday-01",
            "14944019-JUN19-MVS-BUS-Weekday-01",
            "14944022-JUN19-MVS-BUS-Weekday-01",
            "14948211-JUN19-MVS-BUS-Weekday-01",  # additional for 53-111
            "14948218-JUN19-MVS-BUS-Weekday-01",
        ],
    },
    {
        "file": "9_simple_transit_attribute_change.yml",
        "answer": [
            "14940701-JUN19-MVS-BUS-Weekday-01",
            "14943414-JUN19-MVS-BUS-Weekday-01",
            "14943415-JUN19-MVS-BUS-Weekday-01",
            "14946111-JUN19-MVS-BUS-Weekday-01",
            "14946257-JUN19-MVS-BUS-Weekday-01",
            "14946470-JUN19-MVS-BUS-Weekday-01",
            "14946471-JUN19-MVS-BUS-Weekday-01",
            "14946480-JUN19-MVS-BUS-Weekday-01",
            "14946521-JUN19-MVS-BUS-Weekday-01",
            "14947182-JUN19-MVS-BUS-Weekday-01",
            "14947504-JUN19-MVS-BUS-Weekday-01",
            "14947734-JUN19-MVS-BUS-Weekday-01",
            "14947755-JUN19-MVS-BUS-Weekday-01",
            "14978409-JUN19-MVS-BUS-Weekday-01",
            "14981028-JUN19-MVS-BUS-Weekday-01",
            "14981029-JUN19-MVS-BUS-Weekday-01",
            "14986383-JUN19-MVS-BUS-Weekday-01",
            "14986385-JUN19-MVS-BUS-Weekday-01",
        ],
    },
    {
        "file": "9a_multi_transit_attribute_change.yml",
        "answer": [
            "14940701-JUN19-MVS-BUS-Weekday-01",
            "14943414-JUN19-MVS-BUS-Weekday-01",
            "14943415-JUN19-MVS-BUS-Weekday-01",
            "14946111-JUN19-MVS-BUS-Weekday-01",
            "14946257-JUN19-MVS-BUS-Weekday-01",
            "14946470-JUN19-MVS-BUS-Weekday-01",
            "14946471-JUN19-MVS-BUS-Weekday-01",
            "14946480-JUN19-MVS-BUS-Weekday-01",
            "14946521-JUN19-MVS-BUS-Weekday-01",
            "14947182-JUN19-MVS-BUS-Weekday-01",
            "14947504-JUN19-MVS-BUS-Weekday-01",
            "14947734-JUN19-MVS-BUS-Weekday-01",
            "14947755-JUN19-MVS-BUS-Weekday-01",
            "14978409-JUN19-MVS-BUS-Weekday-01",
            "14981028-JUN19-MVS-BUS-Weekday-01",
            "14981029-JUN19-MVS-BUS-Weekday-01",
            "14986383-JUN19-MVS-BUS-Weekday-01",
            "14986385-JUN19-MVS-BUS-Weekday-01",
            "14946199-JUN19-MVS-BUS-Weekday-01",  # add below for Ltd Stop
            "14947598-JUN19-MVS-BUS-Weekday-01",
            "14948211-JUN19-MVS-BUS-Weekday-01",
            "14948218-JUN19-MVS-BUS-Weekday-01",
        ],
    },
]


@pytest.mark.parametrize("selection", TEST_CARD_SELECTIONS)
def test_select_transit_features_from_projectcard(request, selection,stpaul_transit_net,stpaul_card_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    WranglerLogger.debug(f"     Card: {selection['file']}")

    project_card = read_card(os.path.join(stpaul_card_dir, selection['file']))
    sel = project_card.facility

    selected_trips = stpaul_transit_net.select_transit_features(sel)
    WranglerLogger.debug(f"    Exepected Answer: /n{selection['answer']}")
    assert set(selected_trips) == set(selection["answer"])

    WranglerLogger.info(f"--Finished: {request.node.name}")
