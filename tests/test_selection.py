from network_wrangler.selection import build_selection_query
from network_wrangler.logger import WranglerLogger
from network_wrangler.RoadwayNetwork import UNIQUE_MODEL_LINK_IDENTIFIERS
import pytest

# selection, answer
query_tests = [
    # TEST 1
    (
        # SELECTION 1
        {
            "selection": {
                "links": [{"name": ["6th", "Sixth", "sixth"]}],
                "A": {"osm_node_id": "187899923"},  # start searching for segments at A
                "B": {"osm_node_id": "187865924"},  # end at B
            },
            "ignore": [],
        },
        # ANSWER 1
        '((name.str.contains("6th") or '
        + 'name.str.contains("Sixth") or '
        + 'name.str.contains("sixth")) and '
        + "(drive_access==1))",
    ),
    # TEST 2
    (
        # SELECTION 2
        {
            "selection": {
                "links": [{"name": ["6th", "Sixth", "sixth"]}],
                "A": {"osm_node_id": "187899923"},  # start searching for segments at A
                "B": {"osm_node_id": "187865924"},  # end at B
            },
            "ignore": ["name"],
        },
        # ANSWER 1
        "((drive_access==1))",
    ),
    # TEST 3
    (
        # SELECTION 3
        {
            "selection": {
                "links": [
                    {
                        "name": ["6th", "Sixth", "sixth"]
                    },  # find streets that have one of the various forms of 6th
                    {"lanes": [1, 2]},  # only select links that are either 1 or 2 lanes
                    {
                        "bike_access": [1]
                    },  # only select links that are marked for biking
                ],
                "A": {"osm_node_id": "187899923"},  # start searching for segments at A
                "B": {"osm_node_id": "187865924"},  # end at B
            },
            "ignore": [],
        },
        # ANSWER 3
        '((name.str.contains("6th") or '
        + 'name.str.contains("Sixth") or '
        + 'name.str.contains("sixth")) and '
        + "(lanes==1 or lanes==2) and "
        + "(bike_access==1) and (drive_access==1))",
    ),
    # TEST 4
    (
        # SELECTION 4
        {
            "selection": {
                "links": [
                    {
                        "name": ["6th", "Sixth", "sixth"]
                    },  # find streets that have one of the various forms of 6th
                    {"model_link_id": [134574]},
                    {"lanes": [1, 2]},  # only select links that are either 1 or 2 lanes
                    {
                        "bike_access": [1]
                    },  # only select links that are marked for biking
                ],
                "A": {"osm_node_id": "187899923"},  # start searching for segments at A
                "B": {"osm_node_id": "187865924"},  # end at B
            },
            "ignore": [],
        },
        # ANSWER 4
        "((model_link_id==134574))",
    ),
]


@pytest.mark.parametrize("test_spec", query_tests)
def test_query_builder(request, test_spec):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    selection, answer = test_spec
    
    sel_query = build_selection_query(
        selection=selection["selection"],
        unique_ids=UNIQUE_MODEL_LINK_IDENTIFIERS,
        ignore=selection["ignore"],
    )

    print("\nsel_query:\n", sel_query)
    print("\nanswer:\n", answer)
    assert sel_query == answer

    WranglerLogger.info(f"--Finished: {request.node.name}")
