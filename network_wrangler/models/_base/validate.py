"""Wrappers around validation functions that produce more legible output and logging."""

from pandas import DataFrame
from pandera import DataFrameModel
from pandera.errors import SchemaErrors
from pydantic import validate_call
from ...logger import WranglerLogger


class TableValidationError(Exception):
    pass


@validate_call(config=dict(arbitrary_types_allowed=True))
def validate_df_to_model(df: DataFrame, model: DataFrameModel) -> DataFrame:
    """Wrapper to validate a DataFrame against a Pandera DataFrameModel with better logging.

    Args:
        df: DataFrame to validate.
        model: Pandera DataFrameModel to validate against.
    """
    try:
        return model(df)
    except SchemaErrors as e:
        # Log the summary of errors
        WranglerLogger.error(
            f"Validation to {model.__name__} failed with {len(e.failure_cases)} \
            errors: \n{e.failure_cases}"
        )

        # If there are many errors, save them to a file
        if len(e.failure_cases) > 5:
            error_file = "validation_failure_cases.csv"
            e.failure_cases.to_csv(error_file)
            WranglerLogger.info(f"Detailed error cases written to {error_file}")
        else:
            # Otherwise log the errors directly
            WranglerLogger.error("Detailed failure cases:\n%s", e.failure_cases)
        raise TableValidationError(f"Validation to {model.__name__} failed.")
