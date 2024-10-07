"""Tests related to scenarios.

Run just the tests labeled scenario using `tests/test_roadway/test_changes/test_scenario_apply_all_projects.py`
To run with print statments, use `pytest -s tests/test_roadway/test_changes/test_scenario_apply_all_projects.py`
"""

import pytest
from projectcard import ProjectCard, read_card, write_card

from network_wrangler.logger import WranglerLogger
from network_wrangler.scenario import (
    ScenarioConflictError,
    ScenarioCorequisiteError,
    ScenarioPrerequisiteError,
    create_scenario,
)


def test_apply_all_projects(
    request, stpaul_card_dir, stpaul_net, stpaul_transit_net, test_out_dir
):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    stpaul_base_scenario = {
        "road_net": stpaul_net,
        "transit_net": stpaul_transit_net,
    }

    # create 00 scenario and apply project cards
    card_files_00 = [
        "road.prop_change.multiple.yml",
        "road.managed_lane.simple.yml",
    ]

    project_card_path_list_00 = [stpaul_card_dir / filename for filename in card_files_00]

    my_scenario_00 = create_scenario(
        base_scenario=stpaul_base_scenario,
        project_card_filepath=project_card_path_list_00,
    )

    my_scenario_00.apply_all_projects()

    # create 01 scenario and apply project cards
    card_files_01 = [
        "road.prop_change.simple.yml",
        "road.prop_change.widen.yml",
    ]

    project_card_path_list_01 = [stpaul_card_dir / filename for filename in card_files_01]

    my_scenario_01 = create_scenario(
        base_scenario=my_scenario_00,
        project_card_filepath=project_card_path_list_01,
    )

    my_scenario_01.apply_all_projects()

    # write out 01 scenario
    my_scenario_01.write(
        test_out_dir,
        name="v01",
        roadway_file_format="geojson",
        transit_file_format="txt",
        roadway_write=True,
        transit_write=True,
        projects_write=True,
        overwrite=True,
        roadway_convert_complex_link_properties_to_single_field=True,
    )

    WranglerLogger.info(f"--Finished: {request.node.name}")
