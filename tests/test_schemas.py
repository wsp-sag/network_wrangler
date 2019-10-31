import os

import json, yaml
import pytest
from jsonschema import validate

SCHEMA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
    "network_wrangler",
    "schemas",
)

STPAUL_PC_DIR = os.path.join(os.getcwd(), "example", "stpaul", "project_cards")


@pytest.mark.menow
@pytest.mark.travis
def test_project_card_schema():
    schema_filename = os.path.join(SCHEMA_DIR, "project_card_elo.json")
    card_file = os.path.join(STPAUL_PC_DIR, "1_simple_roadway_attribute_change.yml")

    with open(schema_filename) as schema_json_file:
        schema = json.load(schema_json_file)

    with open(card_file, "r") as card:
        card_json = yaml.safe_load(card)

    validate(card_json, schema)
