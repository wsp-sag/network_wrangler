"""Configuration module for network_wrangler."""

from pathlib import Path
from typing import Optional, Union, List

from ..logger import WranglerLogger

from .utils import _update_config_from_files, _config_data_from_files
from .wrangler import WranglerConfig
from .scenario import ScenarioConfig

DefaultConfig = WranglerConfig()

ConfigInputTypes = Union[dict, Path, list[Path], WranglerConfig]


def load_wrangler_config(data: Optional[ConfigInputTypes] = None) -> WranglerConfig:
    """Load the WranglerConfiguration."""
    if isinstance(data, WranglerConfig):
        return data
    config = WranglerConfig()
    if data is None:
        return config
    if isinstance(data, dict):
        return config.update(data)
    elif isinstance(data, Path) or (
        isinstance(data, list) and all(isinstance(d, Path) for d in data)
    ):
        return _update_config_from_files(config, path=data)
    else:
        WranglerLogger.error("No valid configuration data found. Found {data}.")
        raise ValueError("No valid configuration data found.")


def load_scenario_config(
    data: Optional[Union[ScenarioConfig, Path, List[Path], dict]] = None,
) -> ScenarioConfig:
    """Load the WranglerConfiguration."""
    if isinstance(data, ScenarioConfig):
        return data
    if isinstance(data, dict):
        return ScenarioConfig(**data)

    combined_data = _config_data_from_files(data)
    if combined_data is None:
        WranglerLogger.error("No scenario configuration data found in: {data}.")
        raise ValueError("No scenario configuration data found.")

    if isinstance(data, list):
        ex_path = data[0]
    elif isinstance(data, Path):
        ex_path = data
    else:
        ex_path = Path.cwd()

    if ex_path.is_file():
        ex_path = ex_path.parent

    scenario_config = ScenarioConfig(**combined_data, base_path=ex_path)
    return scenario_config
