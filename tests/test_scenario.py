"""Tests related to scenarios.

Run just the tests labeled scenario using `pytest tests/test_scenario.py`
To run with print statments, use `pytest -s tests/test_scenario.py`
"""

import copy

import pytest
from projectcard import ProjectCard, read_card, write_card

from network_wrangler.logger import WranglerLogger
from network_wrangler.scenario import (
    ScenarioConflictError,
    ScenarioCorequisiteError,
    ScenarioPrerequisiteError,
    create_scenario,
    load_scenario,
)


def test_default_config(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    from network_wrangler.configs import DefaultConfig
    from network_wrangler.configs.wrangler import IdGenerationConfig, ModelRoadwayConfig

    assert isinstance(DefaultConfig.IDS, IdGenerationConfig)
    assert isinstance(DefaultConfig.MODEL_ROADWAY, ModelRoadwayConfig)
    assert DefaultConfig.MODEL_ROADWAY.ML_OFFSET_METERS == -10
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_project_card_read(request, stpaul_card_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    in_file = stpaul_card_dir / "road.prop_change.simple.yml"
    project_card = read_card(in_file)
    WranglerLogger.debug(project_card)
    assert project_card.change_type == "roadway_property_change"
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_project_card_write(request, stpaul_card_dir, test_out_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    in_file = stpaul_card_dir / "road.prop_change.simple.yml"
    outfile = test_out_dir / "t_simple_roadway_attribute_change.yml"
    project_card = read_card(in_file)
    write_card(project_card, outfile)
    test_card = read_card(in_file)
    for k, v in project_card.__dict__.items():
        assert v == test_card.__dict__[k]

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_scenario_conflicts(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    project_a = ProjectCard(
        {
            "project": "project a",
            "self_obj_type": "RoadwayNetwork",
            "pycode": "print('hello')",
            "dependencies": {"conflicts": ["project b"]},
        }
    )
    project_b = ProjectCard(
        {"project": "project b", "self_obj_type": "RoadwayNetwork", "pycode": "print('hello')"}
    )

    project_card_list = [project_a, project_b]
    scen = create_scenario(base_scenario={}, project_card_list=project_card_list)

    # should raise an error whenever calling queued projects or when applying them.
    with pytest.raises(ScenarioConflictError):
        WranglerLogger.info(scen.queued_projects)

    with pytest.raises(ScenarioConflictError):
        scen.apply_all_projects()

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_scenario_corequisites(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    project_a = ProjectCard(
        {
            "project": "project a",
            "dependencies": {"corequisites": ["project b", "project c"]},
            "self_obj_type": "RoadwayNetwork",
            "pycode": "print('hello')",
        }
    )
    project_b = ProjectCard(
        {"project": "project b", "self_obj_type": "RoadwayNetwork", "pycode": "print('hello')"}
    )

    project_card_list = [project_a, project_b]
    scen = create_scenario(base_scenario={}, project_card_list=project_card_list)

    # should raise an error whenever calling queued projects or when applying them.
    with pytest.raises(ScenarioCorequisiteError):
        WranglerLogger.info(scen.queued_projects)

    with pytest.raises(ScenarioCorequisiteError):
        scen.apply_all_projects()
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_scenario_prerequisites(request):
    """Shouldn't be able to apply projects if they don't have their pre-requisites applied first."""
    WranglerLogger.info(f"--Starting: {request.node.name}")

    project_a = ProjectCard(
        {
            "project": "project a",
            "dependencies": {"prerequisites": ["project b"]},
            "self_obj_type": "RoadwayNetwork",
            "pycode": "print('hello')",
        }
    )

    project_b = ProjectCard(
        {
            "project": "project b",
            "dependencies": {"prerequisites": ["project c"]},
            "self_obj_type": "RoadwayNetwork",
            "pycode": "print('hello')",
        }
    )

    project_c = ProjectCard(
        {"project": "project c", "self_obj_type": "RoadwayNetwork", "pycode": "print('hello')"}
    )

    project_d = ProjectCard(
        {
            "project": "project d",
            "dependencies": {"prerequisites": ["project b"]},
            "self_obj_type": "RoadwayNetwork",
            "pycode": "print('hello')",
        }
    )
    scen = create_scenario(base_scenario={}, project_card_list=[project_a])

    # should raise an error whenever calling queued projects or when applying them.
    with pytest.raises(ScenarioPrerequisiteError):
        WranglerLogger.info(scen.queued_projects)

    with pytest.raises(ScenarioPrerequisiteError):
        scen.apply_all_projects()

    # add other projects...
    scen.add_project_cards([project_b, project_c, project_d])

    # if apply a project singuarly, it should also fail if it doesn't have prereqs
    with pytest.raises(ScenarioPrerequisiteError):
        scen.apply_projects(["project b"])

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_project_sort(request):
    """Make sure projects sort correctly before being applied."""
    WranglerLogger.info(f"--Starting: {request.node.name}")
    project_a = ProjectCard(
        {
            "project": "project a",
            "dependencies": {"prerequisites": ["project b"]},
            "self_obj_type": "RoadwayNetwork",
            "pycode": "print('hello')",
        }
    )

    project_b = ProjectCard(
        {
            "project": "project b",
            "dependencies": {"prerequisites": ["project c"]},
            "self_obj_type": "RoadwayNetwork",
            "pycode": "print('hello')",
        }
    )

    project_c = ProjectCard(
        {"project": "project c", "self_obj_type": "RoadwayNetwork", "pycode": "print('hello')"}
    )

    project_d = ProjectCard(
        {
            "project": "project d",
            "dependencies": {"prerequisites": ["project b", "project a"]},
            "self_obj_type": "RoadwayNetwork",
            "pycode": "print('hello')",
        }
    )

    expected_project_queue = ["project c", "project b", "project a", "project d"]

    scen = create_scenario(
        base_scenario={},
        project_card_list=[project_a, project_b, project_c, project_d],
    )

    WranglerLogger.debug(f"scen.queued_projects: {scen.queued_projects}")
    assert list(scen.queued_projects) == expected_project_queue

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_apply_summary_wrappers(request, stpaul_card_dir, stpaul_net, stpaul_transit_net):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    stpaul_base_scenario = {
        "road_net": copy.deepcopy(stpaul_net),
        "transit_net": copy.deepcopy(stpaul_transit_net),
    }

    card_files = [
        "road.prop_change.multiple.yml",
        "road.managed_lane.simple.yml",
    ]

    project_card_path_list = [stpaul_card_dir / filename for filename in card_files]

    my_scenario = create_scenario(
        base_scenario=stpaul_base_scenario,
        project_card_filepath=project_card_path_list,
    )

    my_scenario.apply_all_projects()

    WranglerLogger.debug(my_scenario.summary)

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_scenario_write_load(request, small_net, small_transit_net, test_out_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    base_scenario = {
        "road_net": small_net,
        "transit_net": small_transit_net,
        "applied_projects": ["project a", "project b"],
    }
    first_scenario_name = "first_scenario"
    second_scenario_name = "second_scenario"
    first_scenario = create_scenario(base_scenario=base_scenario, name=first_scenario_name)
    scenario_write_dir = test_out_dir / first_scenario_name
    scenario_file_path = first_scenario.write(
        scenario_write_dir, first_scenario_name, projects_write=False
    )
    second_scenario = load_scenario(scenario_file_path, name=second_scenario_name)

    assert second_scenario.applied_projects == first_scenario.applied_projects
    assert second_scenario.road_net.links_df.shape == first_scenario.road_net.links_df.shape
    assert (
        second_scenario.transit_net.feed.trips.shape == first_scenario.transit_net.feed.trips.shape
    )


def test_scenario_building_from_config(request, example_dir, test_out_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    from network_wrangler.scenario import build_scenario_from_config

    scenario_config_file = example_dir / "stpaul" / "myscenario.config.yml"
    scenario = build_scenario_from_config(scenario_config=scenario_config_file)
    assert "365 Bus Reroute".lower() in scenario.applied_projects
    assert (test_out_dir / "myscenario" / "roadway").is_dir()
    assert (test_out_dir / "myscenario" / "projects" / "road.add.simple.yml").is_file()
    assert (test_out_dir / "myscenario" / "transit" / "my_scenario_shapes.txt").is_file()
    assert (test_out_dir / "myscenario" / "my_scenario_scenario.yml").is_file()

    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_tiered_scenario(request, stpaul_card_dir, stpaul_net, stpaul_transit_net, test_out_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    stpaul_base_scenario = {
        "road_net": stpaul_net,
        "transit_net": stpaul_transit_net,
    }

    # create 00 scenario and apply project cards
    card_files_00 = [
        "road.add.simple.yml",
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
    my_scenario_path = my_scenario_01.write(
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

    # test that it can load
    my_loaded_scenario = load_scenario(my_scenario_path, "my_loaded_scenario")
    assert len(my_loaded_scenario.road_net.links_df) == len(my_scenario_01.road_net.links_df)
    assert "6th St E Road Diet".lower() in my_loaded_scenario.applied_projects
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_scenario_apply_highway_and_transit_changes(
    request, stpaul_card_dir, stpaul_net, stpaul_transit_net
):
    WranglerLogger.info(f"--Starting: {request.node.name}")

    stpaul_base_scenario = {
        "road_net": stpaul_net,
        "transit_net": stpaul_transit_net,
    }

    # add roadway project
    card_files = [
        "road.add_and_delete.transit.yml",
        "transit.route_shape_change.yml",
    ]

    my_scenario_00 = create_scenario(
        base_scenario=stpaul_base_scenario,
        project_card_filepath=[stpaul_card_dir / filename for filename in card_files],
    )

    my_scenario_00.apply_all_projects()

    WranglerLogger.info(f"--Finished: {request.node.name}")
