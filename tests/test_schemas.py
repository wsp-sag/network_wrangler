import os

import json

import pytest
import yaml
from jsonschema import validate

SCHEMA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
    "network_wrangler",
    "schemas",
)

@pytest.mark.schema
@pytest.mark.skip(reason="need to work on this")
def test_roadway_link_schema():
    schema_filename = os.path.join(SCHEMA_DIR, "roadway_network_link.json")
    link_file = os.path.join(SMALL_EX_DIR, "link.json")

    with open(schema_filename) as schema_json_file:
        schema = json.load(schema_json_file)

    with open(link_file, "r") as links:
        link_json = yaml.safe_load(links)

    validate(link_json, schema)

