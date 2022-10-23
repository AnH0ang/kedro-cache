from pathlib import Path
from typing import Dict

import pytest
from kedro.framework.project import _ProjectPipelines  # type: ignore
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.pipeline import Pipeline, node
from pytest_mock import MockerFixture

from kedro_cache.config import KedroCacheConfig


@pytest.fixture
def mock_pipeline(mocker: MockerFixture) -> None:
    """Mock the pipeline registry to contain a dummy pipeline."""

    def mocked_register_pipelines() -> Dict[str, Pipeline]:
        return {
            "__default__": Pipeline(
                [node(func=lambda: "foo", inputs=None, outputs="output")]
            )
        }

    mocker.patch.object(
        _ProjectPipelines,
        "_get_pipelines_registry_callable",
        return_value=mocked_register_pipelines,
    )


@pytest.mark.usefixtures("mock_pipeline")
def test_hook_with_missing_cache_config(kedro_project: Path) -> None:
    """Check that a default config is used when the `cache.yml` is missing."""
    # set up project
    bootstrap_project(kedro_project)
    with KedroSession.create(project_path=kedro_project) as session:
        context = session.load_context()

    # check that the default config is used
    assert (
        context.cache_config == KedroCacheConfig()  # type: ignore
    ), "The default config should be used is no `cache.yml` is provided"
