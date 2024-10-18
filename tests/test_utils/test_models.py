"""Tests for /utils/models.

Run just these tests using `pytest tests/test_utils/test_models.py`
"""

import pandas as pd
from pydantic import BaseModel

from network_wrangler.utils.models import (
    coerce_extra_fields_to_type_in_df,
    submodel_fields_in_model,
)


class Submodel(BaseModel):
    submofield1: int

    class Config:
        extra = "allow"


class SampleModel(BaseModel):
    field1: str
    field2: int
    submo: Submodel

    class Config:
        extra = "allow"


def test_submodel_fields_in_model():
    submo = submodel_fields_in_model(SampleModel)
    assert submo == ["submo"]


def test_coerce_extra_fields_to_type_in_df():
    # Create a sample DataFrame
    df = pd.DataFrame(
        {
            "field1": ["value1", "value2"],
            "field2": [1, 2],
            "field3": [True, False],
            "field4": [1, 2],
            "field5": ["1", "2"],
        }
    )

    # Create a sample data instance
    data = SampleModel(
        field1="value3",
        field2=3,
        field3=True,
        submo=Submodel(submofield1=3, field4="4"),
        field5=[5, 7],
    )
    # Call the function
    coerced_data = coerce_extra_fields_to_type_in_df(data, SampleModel, df)

    # Check if the extra field 'field3' is coerced to the type in the DataFrame
    assert isinstance(coerced_data.field3, bool)

    # Check if submodel also coerced
    assert isinstance(coerced_data.submo.field4, int)

    # Check if list values are coerced
    assert coerced_data.field5 == ["5", "7"]
