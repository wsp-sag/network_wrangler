import os

import json, yaml
import pytest
from jsonschema import validate

SCHEMA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
    "network_wrangler",
    "schemas",
)
SMALL_EX_DIR = os.path.join(os.getcwd(), "examples", "single")
STPAUL_EX_DIR = os.path.join(os.getcwd(), "examples", "stpaul")

STPAUL_PC_DIR = os.path.join(os.getcwd(), "examples", "stpaul", "project_cards")


@pytest.mark.schema
@pytest.mark.roadschema
@pytest.mark.skip(reason="need to work on this")
def test_roadway_link_schema():
    schema_filename = os.path.join(SCHEMA_DIR, "roadway_network_link.json")
    link_file = os.path.join(SMALL_EX_DIR, "link.json")

    with open(schema_filename) as schema_json_file:
        schema = json.load(schema_json_file)

    with open(link_file, "r") as links:
        link_json = yaml.safe_load(links)

    validate(link_json, schema)


@pytest.mark.schema
@pytest.mark.skip(reason="need to work on this")
def test_project_card_schema():
    schema_filename = os.path.join(SCHEMA_DIR, "project_card.json")
    card_file = os.path.join(STPAUL_PC_DIR, "1_simple_roadway_attribute_change.yml")

    with open(schema_filename) as schema_json_file:
        schema = json.load(schema_json_file)

    with open(card_file, "r") as card:
        card_json = yaml.safe_load(card)

    validate(card_json, schema)
