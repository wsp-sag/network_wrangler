"""Configuration for parameters for Network Wrangler.

Usage:
    Load the default configuration:

    ```python
    from network_wrangler.configs import DefaultConfig
    ```

    Access the configuration:

    ```python
    from network_wrangler.configs import DefaultConfig
    DefaultConfig.MODEL_ROADWAY.ML_OFFSET_METERS
    >> -10
    ```

    Modify the default configuration in-line:

    ```python
    from network_wrangler.configs import DefaultConfig
    DefaultConfig.MODEL_ROADWAY.ML_OFFSET_METERS = 20
    ```

    Load a configuration from a file:

    ```python
    from network_wrangler.configs import load_wrangler_config
    config = load_wrangler_config("path/to/config.yaml")
    ```

    Set a configuration value:

    ```python
    config.MODEL_ROADWAY.ML_OFFSET_METERS = 10
    ```

Example WranglerConfig file showing the defaults.

```yaml

IDS:
    TRANSIT_SHAPE_ID_METHOD: scalar
    TRANSIT_SHAPE_ID_SCALAR: 1000000
    ROAD_SHAPE_ID_METHOD: scalar
    ROAD_SHAPE_ID_SCALAR: 1000
    ML_LINK_ID_METHOD: "range"
    ML_LINK_ID_RANGE: (950000, 999999)
    ML_LINK_ID_SCALAR: 15000
    ML_NODE_ID_METHOD: "range"
    ML_NODE_ID_RANGE: (950000, 999999)
    ML_NODE_ID_SCALAR: 15000
MODEL_ROADWAY:
    ML_OFFSET_METERS: int = -10
    ADDITIONAL_COPY_FROM_GP_TO_ML: []
    ADDITIONAL_COPY_TO_ACCESS_EGRESS: []
CPU:
    EST_PD_READ_SPEED:
        csv: 0.03
        parquet: 0.005
        geojson: 0.03
        json: 0.15
        txt: 0.04
```


"""
from typing import Literal, Optional

from pydantic.dataclasses import dataclass
from pydantic import Field

from .utils import ConfigItem


@dataclass
class IdGenerationConfig(ConfigItem):
    """Model Roadway Configuration.

    Properties:
        TRANSIT_SHAPE_ID_METHOD: method for creating a shape_id for a transit shape.
            Should be "scalar".
        TRANSIT_SHAPE_ID_SCALAR: scalar value to add to general purpose lane to create a
            shape_id for a transit shape.
        ROAD_SHAPE_ID_METHOD: method for creating a shape_id for a roadway shape.
            Should be "scalar".
        ROAD_SHAPE_ID_SCALAR: scalar value to add to general purpose lane to create a
            shape_id for a roadway shape.
        ML_LINK_ID_METHOD: method for creating a model_link_id for an associated
            link for a parallel managed lane.
        ML_LINK_ID_RANGE: range of model_link_ids to use when creating an associated
            link for a parallel managed lane.
        ML_LINK_ID_SCALAR: scalar value to add to general purpose lane to create a
            model_link_id when creating an associated link for a parallel managed lane.
        ML_NODE_ID_METHOD: method for creating a model_node_id for an associated node
            for a parallel managed lane.
        ML_NODE_ID_RANGE: range of model_node_ids to use when creating an associated
            node for a parallel managed lane.
        ML_NODE_ID_SCALAR: scalar value to add to general purpose lane node ides create
            a model_node_id when creating an associated nodes for parallel managed lane.
    """
    TRANSIT_SHAPE_ID_METHOD: Literal["scalar"] = "scalar"
    TRANSIT_SHAPE_ID_SCALAR: int = 1000000
    ROAD_SHAPE_ID_METHOD: Literal["scalar"] = "scalar"
    ROAD_SHAPE_ID_SCALAR: int = 1000
    ML_LINK_ID_METHOD: Literal["range", "scalar"] = "range"
    ML_LINK_ID_RANGE: tuple[int, int] = (950000, 999999)
    ML_LINK_ID_SCALAR: int = 15000
    ML_NODE_ID_METHOD: Literal["range", "scalar"] = "range"
    ML_NODE_ID_RANGE: tuple[int, int] = (950000, 999999)
    ML_NODE_ID_SCALAR: int = 15000


@dataclass
class ModelRoadwayConfig(ConfigItem):
    """Model Roadway Configuration.

    Properties:
        ML_OFFSET_METERS: Offset in meters for managed lanes.
        ADDITIONAL_COPY_FROM_GP_TO_ML: Additional fields to copy from general purpose to managed
            lanes.
        ADDITIONAL_COPY_TO_ACCESS_EGRESS: Additional fields to copy to access and egress links.
    """
    ML_OFFSET_METERS: int = -10
    ADDITIONAL_COPY_FROM_GP_TO_ML: list[str] = Field(default_factory=list)
    ADDITIONAL_COPY_TO_ACCESS_EGRESS: list[str] = Field(default_factory=list)


@dataclass
class CpuConfig(ConfigItem):
    """CPU Configuration -  Will not change any outcomes.

    Properties:
        EST_PD_READ_SPEED: Read sec / MB - WILL DEPEND ON SPECIFIC COMPUTER
    """
    EST_PD_READ_SPEED: dict[str, float] = Field(default_factory=lambda: {
        "csv": 0.03,
        "parquet": 0.005,
        "geojson": 0.03,
        "json": 0.15,
        "txt": 0.04,
    })


@dataclass
class WranglerConfig(ConfigItem):
    """Configuration for Network Wrangler.

    Properties:
        IDS: Parameteters governing how new ids are generated.
        MODEL_ROADWAY: Parameters governing how the model roadway is created.
        CPU: Parameters for accessing CPU information. Will not change any outcomes.

    """
    IDS: IdGenerationConfig = IdGenerationConfig()
    MODEL_ROADWAY: ModelRoadwayConfig = ModelRoadwayConfig()
    CPU: CpuConfig = CpuConfig()
