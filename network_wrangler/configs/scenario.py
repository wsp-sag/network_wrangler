"""Scenario configuration for Network Wrangler.

Usage:
    Load a configuration from a file:

    ```python
    from network_wrangler.configs import load_scenario_config
    my_scenario_config = load_scenario_config("path/to/config.yaml")
    ```

    Access the configuration:

    ```python
    my_scenario_config.base_transit_network.path
    >> path/to/transit_network
    ```

    Build a network using scenario:

    ```python
    from scenario import build_scenario_from_config
    my_scenario = build_scenario_from_config(my_scenario_config)
    ```

Example configuration file:

```yaml
name: "my_scenario"
base_scenario:
    roadway:
        dir: "path/to/roadway_network"
        suffix: "geojson"
        read_in_shapes: True
    transit:
        dir: "path/to/transit_network"
        suffix: "txt"
    applied_projects:
        - "project1"
        - "project2"
    conflicts:
        - "project3"
        - "project4"
projects:
    project_card_filepath:
        - "path/to/projectA.yaml"
        - "path/to/projectB.yaml"
    filter_tags:
        - "tag1"
output_scenario:
    overwrite: True
    roadway:
        out_dir: "path/to/output/roadway"
        prefix: "my_scenario"
        file_format: "geojson"
        true_shape: False
    transit:
        out_dir: "path/to/output/transit"
        prefix: "my_scenario"
        file_format: "txt"
    project_cards:
        out_dir: "path/to/output/project_cards"

wrangler_config: "path/to/wrangler_config.yaml"
```

"""
from datetime import datetime
from pathlib import Path
from typing import Optional, Union


from ..models._base.types import RoadwayFileTypes, TransitFileTypes
from .utils import ConfigItem
from .wrangler import WranglerConfig


class ProjectsConfig(ConfigItem):
    """Configuration for projects in a scenario.

    Attributes:
        project_card_filepath: where the project card is.  A single path, list of paths,
            a directory, or a glob pattern. Defaults to None.
        filter_tags: List of tags to filter the project cards by.
    """
    def __init__(
        self,
        base_path: Path = Path.cwd(),
        project_card_filepath: Union[Path, list[Path], str, list[str]] = [],
        filter_tags: list[str] = [],
    ):
        """Constructor for ProjectsConfig."""
        if isinstance(project_card_filepath, list):
            self.project_card_filepath = []
            for p in project_card_filepath:
                p = Path(p)
                if not p.is_absolute():
                    self.project_card_filepath.append((base_path / p).resolve())
                else:
                    self.project_card_filepath.append(p)
        elif not Path(project_card_filepath).is_absolute():
            self.project_card_filepath = (base_path / Path(project_card_filepath)).resolve()
        else:
            self.project_card_filepath = Path(project_card_filepath)
        self.filter_tags = filter_tags


class RoadwayNetworkInputConfig(ConfigItem):
    """Configuration for the road network in a scenario.

    Attributes:
        dir: Path to directory with roadway network files.
        suffix: File suffix for the roadway network files. Should be one of RoadwayFileTypes.
            Defaults to "geojson".
        read_in_shapes: If True, will read in the shapes of the roadway network. Defaults to False.
        boundary_geocode: Geocode of the boundary. Will use this to filter the roadway network.
        boundary_file: Path to the boundary file. If provided and both boundary_gdf and
            boundary_geocode are not provided, will use this to filter the roadway network.
    """
    def __init__(
        self,
        base_path: Path = Path.cwd(),
        dir: Path = Path("."),
        suffix: RoadwayFileTypes = "geojson",
        read_in_shapes: bool = False,
        boundary_geocode: Optional[str] = None,
        boundary_file: Optional[Path] = None,
    ):
        """Constructor for RoadwayNetworkInputConfig."""
        if dir is not None and not Path(dir).is_absolute():
            self.dir = (base_path / Path(dir)).resolve()
        else:
            self.dir = Path(dir)
        self.suffix = suffix
        self.read_in_shapes = read_in_shapes
        self.boundary_geocode = boundary_geocode
        self.boundary_file = boundary_file


class RoadwayNetworkOutputConfig(ConfigItem):
    """Configuration for writing out the resulting roadway network for a scenario.

    Attributes:
        out_dir: Path to write the roadway network files to if you don't want to use the default.
        prefix: Prefix to add to the file name. If not provided will use the scenario name.
        file_format: File format to write the roadway network to. Should be one of
            RoadwayFileTypes. Defaults to "geojson".
        true_shape: If True, will write the true shape of the roadway network. Defaults to False.
        write: If True, will write the roadway network. Defaults to True.
    """
    def __init__(
        self,
        out_dir: Path = Path("./roadway"),
        base_path: Path = Path.cwd(),
        convert_complex_link_properties_to_single_field: bool = False,
        prefix: Optional[str] = None,
        file_format: RoadwayFileTypes = "geojson",
        true_shape: bool = False,
        write: bool = True,
    ):
        """Constructor for RoadwayNetworkOutputConfig."""
        if out_dir is not None and not Path(out_dir).is_absolute():
            self.out_dir = (base_path / Path(out_dir)).resolve()
        else:
            self.out_dir = Path(out_dir)

        self.convert_complex_link_properties_to_single_field = convert_complex_link_properties_to_single_field
        self.prefix = prefix
        self.file_format = file_format
        self.true_shape = true_shape
        self.write = write


class TransitNetworkInputConfig(ConfigItem):
    """Configuration for the transit network in a scenario.

    Attributes:
        dir: Path to the transit network files. Defaults to ".".
        suffix: File suffix for the transit network files. Should be one of TransitFileTypes.
            Defaults to "txt".
    """
    def __init__(
        self,
        base_path: Path = Path.cwd(),
        dir: Path = Path("."),
        suffix: TransitFileTypes = "txt",
    ):
        """Constructor for TransitNetworkInputConfig."""
        if dir is not None and not Path(dir).is_absolute():
            self.feed = (base_path / Path(dir)).resolve()
        else:
            self.feed = Path(dir)
        self.suffix = suffix


class TransitNetworkOutputConfig(ConfigItem):
    """Configuration for the transit network in a scenario.

    Attributes:
        out_dir: Path to write the transit network files to if you don't want to use the default.
        prefix: Prefix to add to the file name. If not provided will use the scenario name.
        file_format: File format to write the transit network to. Should be one of
            TransitFileTypes. Defaults to "txt".
        write: If True, will write the transit network. Defaults to True.
    """
    def __init__(
        self,
        base_path: Path = Path.cwd(),
        out_dir: Path = Path("./transit"),
        prefix: Optional[str] = None,
        file_format: TransitFileTypes = "txt",
        write: bool = True,
    ):
        """Constructor for TransitNetworkOutputCOnfig."""
        if out_dir is not None and not Path(out_dir).is_absolute():
            self.out_dir = (base_path / Path(out_dir)).resolve()
        else:
            self.out_dir = Path(out_dir)
        self.write = write
        self.prefix = prefix
        self.file_format = file_format


class ProjectCardOutputConfig(ConfigItem):
    """Configuration for outputing project cards in a scenario.

    Attributes:
        out_dir: Path to write the project card files to if you don't want to use the default.
        write: If True, will write the project cards. Defaults to True.
    """
    def __init__(
        self,
        base_path: Path = Path.cwd(),
        out_dir: Path = Path("./projects"),
        write: bool = True
    ):
        """Constructor for ProjectCardOutputConfig."""
        if out_dir is not None and not Path(out_dir).is_absolute():
            self.out_dir = (base_path / Path(out_dir)).resolve()
        else:
            self.out_dir = Path(out_dir)
        self.write = write


class ScenarioInputConfig(ConfigItem):
    """Configuration for the writing the output of a scenario.

    Attributes:
        roadway: Configuration for writing out the roadway network.
        transit: Configuration for writing out the transit network.
        applied_projects: List of projects to apply to the base scenario.
        conflicts: List of projects that conflict with the applied_projects.
    """
    def __init__(
        self,
        base_path: Path = Path.cwd(),
        roadway: Optional[dict] = None,
        transit: Optional[dict] = None,
        applied_projects: list[str] = [],
        conflicts: dict = {},
    ):
        """Constructor for ScenarioInputConfig."""
        if roadway is not None:
            self.roadway = RoadwayNetworkInputConfig(**roadway, base_path=base_path)
        else:
            self.roadway = None

        if transit is not None:
            self.transit = TransitNetworkInputConfig(**transit, base_path=base_path)
        else:
            self.transit = None

        self.applied_projects = applied_projects
        self.conflicts = conflicts


class ScenarioOutputConfig(ConfigItem):
    """Configuration for the writing the output of a scenario.

    Attributes:
        roadway: Configuration for writing out the roadway network.
        transit: Configuration for writing out the transit network.
        project_cards: Configuration for writing out the project cards.
        overwrite: If True, will overwrite the files if they already exist. Defaults to True
    """
    def __init__(
        self,
        path: Path = Path("./output"),
        base_path: Path = Path.cwd(),
        roadway: dict = RoadwayNetworkOutputConfig().to_dict(),
        transit: dict = TransitNetworkOutputConfig().to_dict(),
        project_cards: Optional[ProjectCardOutputConfig] = None,
        overwrite: bool = True
    ):
        """Constructor for ScenarioOutputConfig."""
        if not Path(path).is_absolute():
            self.path = (base_path / Path(path)).resolve()
        else:
            self.path = Path(path)

        self.roadway = RoadwayNetworkOutputConfig(**roadway, base_path=self.path)
        self.transit = TransitNetworkOutputConfig(**transit, base_path=self.path)

        if project_cards is not None:
            self.project_cards = ProjectCardOutputConfig(**project_cards, base_path=self.path)
        else:
            self.project_cards = None

        self.overwrite = overwrite


class ScenarioConfig(ConfigItem):
    """Scenario configuration for Network Wrangler.

    Attributes:
        base_path: base path of the scenario. Defaults to cwd.
        name: Name of the scenario.
        base_scenario: information about the base scenario
        projects: information about the projects to apply on top of the base scenario
        output_scenario: information about how to output the scenario
        wrangler_config: wrangler configuration to use
    """
    def __init__(
        self,
        base_scenario: dict,
        projects: dict,
        output_scenario: dict,
        base_path: Path = Path.cwd,
        name: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        wrangler_config=WranglerConfig()
    ):
        """Constructor for ScenarioConfig."""
        self.base_path = Path(base_path)
        self.name = name
        self.base_scenario = ScenarioInputConfig(**base_scenario, base_path=base_path)
        self.projects = ProjectsConfig(**projects, base_path=base_path)
        self.output_scenario = ScenarioOutputConfig(**output_scenario, base_path=base_path)
        self.wrangler_config = wrangler_config
