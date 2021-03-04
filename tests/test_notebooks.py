import os
import glob
import pytest
from pytest_notebook.nb_regression import NBRegressionFixture

# NOTE IF HAVING TROUBLE, MAKE SURE you have "nbconvert~=5.6.0"

NOTEBOOK_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "notebook"
)


@pytest.mark.notebooks
def test_notebooks():
    fixture = NBRegressionFixture(
        exec_timeout=50,
        diff_ignore=(
            "/cells/*/execution_count",
            "/metadata/language_info/version",
            "/cells/*/outputs",
        ),
    )

    for file in glob.glob(os.path.join(NOTEBOOK_DIR, "*.ipynb")):
        print(file)
        fixture.check(str(file))
