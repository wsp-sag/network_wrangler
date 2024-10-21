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
