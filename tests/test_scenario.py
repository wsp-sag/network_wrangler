import os
import pytest

"""
Run just the tests labeled basic using `pytest -v -m basic`
"""

@pytest.mark.basic
@pytest.mark.schema
def test_project_card_schema():
    from network_wrangler import ProjectCard
    dir = os.path.join(os.getcwd(),'example','stpaul','project_cards')
    simple_projectcard_file = os.path.join(dir,"1_simple_roadway_attribute_change.yml")
    pc = ProjectCard(simple_projectcard_file)
