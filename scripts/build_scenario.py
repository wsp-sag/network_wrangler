import os
import sys
import yaml
import warnings

from network_wrangler import Scenario
from network_wrangler.scenario import create_base_scenario


warnings.filterwarnings("ignore")

if __name__ == "__main__":
    args = sys.argv

    if len(args) == 1:
        raise ValueError("ERROR - config file must be passed as an argument!!!")

    config_file = args[1]

    if not os.path.exists(config_file):
        raise FileNotFoundError(
            "Specified config file does not exists - {}".format(config_file)
        )

    with open(config_file, "r") as config:
        config_dict = yaml.safe_load(config)

    base_network_dir = config_dict.get("base_network").get("input_dir")
    base_shape_name = config_dict.get("base_network").get("shape_file_name")
    base_link_name = config_dict.get("base_network").get("link_file_name")
    base_node_name = config_dict.get("base_network").get("node_file_name")
    validate_base_network = config_dict.get("base_network").get("validate_network")

    project_card_filepath = config_dict.get("scenario").get("project_card_filepath")
    project_tags = config_dict.get("scenario").get("tags")

    write_out = config_dict.get("scenario").get("write_out")
    out_dir = config_dict.get("scenario").get("output_dir")
    out_prefix = config_dict.get("scenario").get("out_prefix")

    if project_tags is None:
        project_tags = []

    if project_card_filepath is None:
        project_card_filepath = []

    # Create Base Network
    base_scenario = create_base_scenario(
        highway_dir=base_network_dir,
        base_shape_name=base_shape_name,
        base_link_name=base_link_name,
        base_node_name=base_node_name,
        validate=validate_base_network,
        transit_dir=base_network_dir,
    )

    my_scenario = Scenario.create_scenario(
        base_scenario=base_scenario,
        project_card_filepath=project_card_filepath,
        filter_tags=project_tags,
        validate=False,
    )

    print("Applying these projects to the base scenario ...")
    print("\n".join(my_scenario.get_project_names()))

    my_scenario.apply_all_projects()

    if write_out:
        my_scenario.road_net.write(filename=out_prefix, path=out_dir)
        my_scenario.transit_net.write(filename=out_prefix, path=out_dir)
