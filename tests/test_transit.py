import os
import json
import pytest
from network_wrangler import TransitNetwork
from network_wrangler import ProjectCard


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


@pytest.mark.transit
@pytest.mark.travis
def test_select_transit_features_from_projectcard(request):
    print("\n--Starting:", request.node.name)
    net = TransitNetwork.read(STPAUL_DIR)

    test_selections = [
        {
            'file': '7_simple_transit_attribute_change.yml',
            'answer': ['14940701-JUN19-MVS-BUS-Weekday-01']
        },
        {
            'file': '8_simple_transit_attribute_change.yml',
            'answer': [
                '14944012-JUN19-MVS-BUS-Weekday-01',
                '14944019-JUN19-MVS-BUS-Weekday-01'
            ]
        },
        {
            'file': '9_simple_transit_attribute_change.yml',
            'answer': [
                '14940701-JUN19-MVS-BUS-Weekday-01',
                '14943414-JUN19-MVS-BUS-Weekday-01',
                '14943415-JUN19-MVS-BUS-Weekday-01',
                '14946111-JUN19-MVS-BUS-Weekday-01',
                '14946257-JUN19-MVS-BUS-Weekday-01',
                '14946470-JUN19-MVS-BUS-Weekday-01',
                '14946471-JUN19-MVS-BUS-Weekday-01',
                '14946480-JUN19-MVS-BUS-Weekday-01',
                '14946521-JUN19-MVS-BUS-Weekday-01',
                '14947182-JUN19-MVS-BUS-Weekday-01',
                '14947504-JUN19-MVS-BUS-Weekday-01',
                '14947734-JUN19-MVS-BUS-Weekday-01',
                '14947755-JUN19-MVS-BUS-Weekday-01',
                '14978409-JUN19-MVS-BUS-Weekday-01',
                '14981028-JUN19-MVS-BUS-Weekday-01',
                '14981029-JUN19-MVS-BUS-Weekday-01',
                '14986383-JUN19-MVS-BUS-Weekday-01',
                '14986385-JUN19-MVS-BUS-Weekday-01'
            ]
        }
    ]

    for i, test in enumerate(test_selections):
        print("--->", i)
        print("Reading project card", test['file'], "...")

        project_card_path = os.path.join(
            STPAUL_DIR, 'project_cards', test['file']
        )
        project_card = ProjectCard.read(project_card_path)
        sel = project_card.facility

        selected_trips = net.select_transit_features(sel)
        assert(set(selected_trips) == set(test['answer']))

    print("--Finished:", request.node.name)


@pytest.mark.transit
@pytest.mark.travis
def test_wrong_existing(request):
    print("\n--Starting:", request.node.name)
    net = TransitNetwork.read(STPAUL_DIR)

    selected_trips = net.select_transit_features({
        'trip_id': [
            '14944018-JUN19-MVS-BUS-Weekday-01',
            '14944012-JUN19-MVS-BUS-Weekday-01'
        ]
    })

    with pytest.raises(Exception):
        net.apply_transit_feature_change(
            selected_trips,
            [
                {
                    'property': 'headway_secs',
                    'existing': 553,
                    'set': 900
                }
            ]
        )

    print("--Finished:", request.node.name)


@pytest.mark.transit
@pytest.mark.travis
def test_zero_valid_facilities(request):
    print("\n--Starting:", request.node.name)
    net = TransitNetwork.read(STPAUL_DIR)

    with pytest.raises(Exception):
        net.select_transit_features({
            'trip_id': [
                '14941433-JUN19-MVS-BUS-Weekday-01'
            ],
            'time': [
                '06:00:00',
                '09:00:00'
            ]
        })

    print("--Finished:", request.node.name)


@pytest.mark.transit
@pytest.mark.travis
def test_invalid_selection_key(request):
    print("\n--Starting:", request.node.name)
    net = TransitNetwork.read(STPAUL_DIR)

    with pytest.raises(Exception):
        # trip_ids rather than trip_id should fail
        net.select_transit_features({
            'trip_ids': [
                '14941433-JUN19-MVS-BUS-Weekday-01'
            ]
        })

    print("--Finished:", request.node.name)


if __name__ == '__main__':
    test_transit_read_write()
