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


@pytest.mark.travis
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
        },
        "3. route_id": {
            'route_id': '365-111',
            'answer': ['14947182-JUN19-MVS-BUS-Weekday-01']
        },

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
def test_apply_transit_feature_change_from_projectcard(request):
    print("\n--Starting:", request.node.name)
    net = TransitNetwork.read(STPAUL_DIR)

    test_selections = [
        {
            'file': '7_simple_transit_attribute_change.yml',
            'answer': {
                'trip_ids': ['14940701-JUN19-MVS-BUS-Weekday-01'],
                'headway_secs': [900]
            }
        },
        {
            'file': '8_simple_transit_attribute_change.yml',
            'answer': {
                'trip_ids': [
                    '14944012-JUN19-MVS-BUS-Weekday-01',
                    '14944019-JUN19-MVS-BUS-Weekday-01'
                ],
                'headway_secs': [
                    253,
                    226
                ]
            }
        },
        {
            'file': '9_simple_transit_attribute_change.yml',
            'answer': {
                'trip_ids': [
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
                ],
                'headway_secs': [1800]
            }
        }
    ]

    for i, test in enumerate(test_selections):
        print("--->", i)
        print("Reading project card", test['file'], "...")

        project_card_path = os.path.join(
            STPAUL_DIR, 'project_cards', test['file']
        )
        project_card = ProjectCard.read(project_card_path)
        net.apply_transit_feature_change(
            net.select_transit_features(project_card.facility),
            project_card.properties
        )

        freq = net.feed.frequencies
        answers = test['answer']['headway_secs']
        if len(answers) > 1:
            for i, answer in enumerate(answers):
                match = (freq.trip_id == test['answer']['trip_ids'][i])
                result = freq[match]['headway_secs']
                assert(set(result) == set([answer]))
        else:
            matches = freq.trip_id.isin(test['answer']['trip_ids'])
            results = freq[matches]['headway_secs'].tolist()
            assert(set(results) == set(answers))

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
