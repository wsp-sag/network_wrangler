"""Configuration utilities."""
from abc import ABC

from pathlib import Path
from typing import List, Union, Optional
from pydantic import ValidationError

from ..utils.io_dict import load_merge_dict, load_dict
from ..logger import WranglerLogger

SUPPORTED_CONFIG_EXTENSIONS = ['.yml', '.yaml', '.json', '.toml']


class ConfigItem(ABC):
    """Base class to add partial dict-like interface to  configuration.

    Allow use of .items() ["X"] and .get("X") .to_dict() from configuration.

    Not to be constructed directly. To be used a mixin for dataclasses
    representing config schema.
    Do not use "get" "to_dict", or "items" for key names.
    """
    base_path = None

    def __getitem__(self, key):
        """Return the value for key if key is in the dictionary, else default."""
        return getattr(self, key)

    def items(self):
        """A set-like object providing a view on D's items."""
        return self.__dict__.items()

    def to_dict(self):
        """Convert the configuration to a dictionary."""
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, ConfigItem):
                result[key] = value.to_dict()
            else:
                result[key] = value
        return result

    def get(self, key, default=None):
        """Return the value for key if key is in the dictionary, else default."""
        return self.__dict__.get(key, default)

    def update(self, data: Union[Path, list[Path], dict]):
        """Update the configuration with a dictionary of new values."""
        if not isinstance(data, dict):
            WranglerLogger.info(f"Updating configuration with {data}.")
            data = load_merge_dict(data)

        self.__dict__.update(data)
        return self

    def resolve_paths(self, base_path):
        """Resolve relative paths in the configuration."""
        base_path = Path(base_path)
        for key, value in self.__dict__.items():
            if isinstance(value, ConfigItem):
                value.resolve_paths(base_path)
            elif isinstance(value, str) and value.startswith("."):
                resolved_path = (base_path / value).resolve()
                setattr(self, key, str(resolved_path))


def find_configs_in_dir(dir: Union[Path, list[Path]], config_type) -> list[Path]:
    """Find configuration files in the directory that match `*config<ext>`."""
    config_files: list[Path] = []
    if isinstance(dir, list):
        for d in dir:
            config_files.extend(find_configs_in_dir(d))
    elif dir.is_dir():
        dir = Path(dir)
        for ext in SUPPORTED_CONFIG_EXTENSIONS:
            config_like_files = dir.glob(f"*config{ext}")
            config_files.extend(find_configs_in_dir(config_like_files))
    elif dir.is_file():
        try:
            config_type(load_dict(dir))
        except ValidationError:
            return config_files
        config_files.append(dir)

    if config_files:
        return [Path(config_file) for config_file in config_files]
    return []


def _config_data_from_files(path: Optional[Union[Path, List[Path]]] = None) -> Union[None, dict]:
    """Load and combine configuration data from file(s).

    Args:
        path: a valid system path to a config file or list of paths.
    """
    if path is None:
        path = [Path.cwd()]
    elif not isinstance(path, list):
        path = [path]

    if all(p.is_dir() for p in path):
        config_files = find_configs_in_dir(path, ConfigItem)
    elif all(p.is_file() for p in path):
        config_files = path
    else:
        WranglerLogger.error(f"All paths must be directories or files, not mixed. Found: {path}")
        raise ValueError("All paths must be directories or files, not mixed.")

    if len(config_files) == 0:
        WranglerLogger.info(
            f"No configuration files found in {path}. Using default configuration."
        )
        return None

    data = load_merge_dict(config_files)
    return data


def _update_config_from_files(config, config_type, path: Optional[Union[Path, List[Path]]]):
    """Load configuration from files(s) which updates the default configuration.

    Args:
        config: a Configuration object to update with the new configuration.
        config_type: a class representing the configuration schema.
        path: a valid system path to a config file or list of paths.

    Returns:
        A Configuration object
    """
    config_files = _config_data_from_files(path, config_type)
    if config_files is None:
        return config
    return config.update(config_files)
