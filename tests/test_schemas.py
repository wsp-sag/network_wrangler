import json

import pytest

from jsonschema import Draft7Validator

from network_wrangler import WranglerLogger


def test_schemas(request, schema_filename):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    if schema_filename is None:
        pytest.skip()
    with open(schema_filename) as schema_json_file:
        schema = json.load(schema_json_file)

    Draft7Validator.check_schema(schema)
    WranglerLogger.info(f"--Finished: {request.node.name}")
