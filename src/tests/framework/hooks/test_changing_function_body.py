import time
from pathlib import Path
from typing import Any, Callable, Dict

import pandas as pd
import pytest
from contexttimer import Timer
from kedro.extras.datasets.pickle import PickleDataSet
from kedro.framework.context import KedroContext
from kedro.framework.project import _ProjectPipelines  # type: ignore
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.io import DataCatalog, MemoryDataSet
from kedro.pipeline.modular_pipeline import pipeline
from kedro.pipeline.node import node
from kedro.pipeline.pipeline import Pipeline
from pytest import MonkeyPatch
from pytest_mock import MockerFixture


@pytest.fixture
def mock_catalog(mocker: MockerFixture) -> None:
    """Mock a the data catalog.

    Args:
        mocker: A pytest mocker fixture.
    """
    dummy_catalog = DataCatalog(
        {
            "input": MemoryDataSet(pd.DataFrame({"a": [1, 2, 3]})),  # type: ignore
            "output": PickleDataSet("output.pkl"),
        }
    )

    mocker.patch.object(
        KedroContext,
        "_get_catalog",
        return_value=dummy_catalog,
    )


@pytest.fixture
def original_pipeline_registry() -> Callable[[], Dict[str, Pipeline]]:
    """Mock a pipeline registry function.

    Returns:
        A function that returns the original pipeline registry.
    """

    def foo(x: Any) -> Any:
        print("original")
        time.sleep(2)
        return x

    def mocked_register_pipelines() -> Dict[str, Pipeline]:
        test_pipeline = pipeline([node(func=foo, inputs=["input"], outputs="output")])
        return {"__default__": test_pipeline}

    return mocked_register_pipelines


@pytest.fixture
def changed_pipeline_registry() -> Callable[[], Dict[str, Pipeline]]:
    """Mock a pipeline registry function.

    Returns:
        A function that returns the original pipeline registry.
    """

    def foo(x: Any) -> Any:
        print("changed")
        time.sleep(2)
        return x

    def mocked_register_pipelines() -> Dict[str, Pipeline]:
        test_pipeline = pipeline([node(func=foo, inputs=["input"], outputs="output")])
        return {"__default__": test_pipeline}

    return mocked_register_pipelines


@pytest.mark.usefixtures("mock_catalog")
def test_no_caching_when_function_body_changes(
    mocker: MockerFixture,
    monkeypatch: MonkeyPatch,
    kedro_project_with_cache_config: Path,
    original_pipeline_registry: Callable[[], Dict[str, Pipeline]],
    changed_pipeline_registry: Callable[[], Dict[str, Pipeline]],
) -> None:
    """Check that the cache is not used when the function body changes."""
    session: KedroSession

    # change dir
    monkeypatch.chdir(kedro_project_with_cache_config)

    # first run
    mocker.patch.object(
        _ProjectPipelines,
        "_get_pipelines_registry_callable",
        return_value=original_pipeline_registry,
    )
    bootstrap_project(kedro_project_with_cache_config)
    with Timer() as first_t:
        with KedroSession.create(
            project_path=kedro_project_with_cache_config
        ) as session:
            session.run()

    # second run
    mocker.patch.object(
        _ProjectPipelines,
        "_get_pipelines_registry_callable",
        return_value=changed_pipeline_registry,
    )
    bootstrap_project(kedro_project_with_cache_config)
    with Timer() as second_t:
        with KedroSession.create(
            project_path=kedro_project_with_cache_config
        ) as session:
            session.run()

    # check that there was no caching
    time_diff = abs(first_t.elapsed - second_t.elapsed)
    assert time_diff < 0.3, "The node was cached, but it shouldn't have been."
